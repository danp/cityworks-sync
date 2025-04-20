package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/embed"
)

func main() {
	ctx := context.Background()

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	var dbPath string
	fs.StringVar(&dbPath, "db", "data.db", "database file path")
	var requestsFile, fieldsFile string
	fs.StringVar(&requestsFile, "requests", "", "requests file path, otherwise download")
	fs.StringVar(&fieldsFile, "fields", "", "fields file path, otherwise download")
	fs.Parse(os.Args[1:])

	if err := run(ctx, dbPath, requestsFile, fieldsFile); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, dbPath, requestsFile, fieldsFile string) error {
	db, err := sql.Open("sqlite3", "file:"+dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS sync_state (requests_modified DATETIME)`); err != nil {
		return fmt.Errorf("creating sync_state table: %w", err)
	}

	halifax, err := time.LoadLocation("America/Halifax")
	if err != nil {
		return fmt.Errorf("loading location: %w", err)
	}

	yearTables := make(map[int]struct{})

	var requests io.ReadCloser
	var requestsModified time.Time
	if requestsFile != "" {
		requests, err = os.Open(requestsFile)
		if err != nil {
			return fmt.Errorf("opening data file: %w", err)
		}
	} else {
		var modified time.Time
		if err := db.QueryRow("SELECT requests_modified FROM sync_state").Scan(&modified); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("selecting modified time: %w", err)
			}
		}

		const requestsID = "d2b7dd138adb468293183926a1a7a81c"
		r, modified, err := download(ctx, requestsID, modified)
		if err != nil {
			return fmt.Errorf("downloading data: %w", err)
		}
		if modified.IsZero() {
			return nil
		}
		requests = r
		requestsModified = modified
	}
	defer requests.Close()

	cr := csv.NewReader(requests)

	// REQUEST_ID DATE_INITIATED DATE_CLOSED DESCRIPTION INITIATED_BY PRIORITY ADDRESS COMMUNITY DISTRICT REQUEST_CATEGORY RESOLUTION LATITUDE LONGITUDE STATUS DEPT_RESPONSIBILITY WORK_ORDER ObjectId PROJECT_NAME
	header, err := cr.Read()
	if err != nil {
		return fmt.Errorf("reading header: %w", err)
	}
	headerIndices := make(map[string]int)
	for i, h := range header {
		headerIndices[h] = i
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback()

	requestYearStmts := make(map[int]*sql.Stmt)
	requestYearStmt := func(year int) (*sql.Stmt, error) {
		if stmt, ok := requestYearStmts[year]; ok {
			return stmt, nil
		}

		mainColumns := []string{
			"id",
			"initiated",
			"closed",
			"description",
			"initiator",
			"priority",
			"address",
			"community",
			"district",
			"category",
			"resolution",
			"latitude",
			"longitude",
			"status",
			"department",
			"work_order",
			"project_name",
		}
		insertColumns := []string{
			"first_observed",
		}

		var insertSQL strings.Builder
		fmt.Fprintf(&insertSQL, `INSERT INTO requests_%d (`, year)
		for i, c := range mainColumns {
			if i > 0 {
				insertSQL.WriteString(",")
			}
			insertSQL.WriteString(c)
		}
		for _, c := range insertColumns {
			insertSQL.WriteString(",")
			insertSQL.WriteString(c)
		}
		insertSQL.WriteString(`) VALUES (`)
		for i := range len(mainColumns) + len(insertColumns) {
			if i > 0 {
				insertSQL.WriteString(",")
			}
			insertSQL.WriteString("?")
		}
		insertSQL.WriteString(`) ON CONFLICT (id) DO UPDATE SET `)
		for i, c := range mainColumns {
			if i > 0 {
				insertSQL.WriteString(",")
			}
			insertSQL.WriteString(c + "=excluded." + c)
		}

		stmt, err := tx.Prepare(insertSQL.String())
		if err != nil {
			return nil, fmt.Errorf("preparing insert statement: %w", err)
		}
		requestYearStmts[year] = stmt
		return stmt, nil
	}

	requestYears := make(map[int]int) // request ID -> year

	var n int
	for {
		row, err := cr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading row: %w", err)
		}

		requestID, err := strconv.Atoi(row[headerIndices["REQUEST_ID"]])
		if err != nil {
			return fmt.Errorf("parsing request ID: %w", err)
		}

		const timeFormat = "1/2/2006 3:04:05 PM"
		var initiatedValue string
		initiated, err := time.ParseInLocation(timeFormat, row[headerIndices["DATE_INITIATED"]], halifax)
		if err != nil {
			return fmt.Errorf("parsing initiated time: %w", err)
		}
		initiatedValue = initiated.UTC().Format(time.RFC3339)
		var closed time.Time
		var closedValue string
		if v := row[headerIndices["DATE_CLOSED"]]; v != "" {
			t, err := time.ParseInLocation(timeFormat, v, halifax)
			if err != nil {
				return fmt.Errorf("parsing closed time: %w", err)
			}
			closed = t
			closedValue = t.UTC().Format(time.RFC3339)
		}

		if initiated.Before(time.Date(2000, 1, 1, 0, 0, 0, 0, halifax)) {
			if closed.IsZero() {
				return fmt.Errorf("invalid initiated time: %v %v", row[headerIndices["REQUEST_ID"]], initiated)
			}
			initiated = closed
		}

		year := initiated.Year()
		if _, ok := yearTables[year]; !ok {
			if err := createYearTable(tx, year); err != nil {
				return fmt.Errorf("creating year table: %w", err)
			}
			yearTables[year] = struct{}{}
		}

		requestYears[requestID] = year

		stmt, err := requestYearStmt(year)
		if err != nil {
			return fmt.Errorf("getting year statement: %w", err)
		}

		_, err = stmt.Exec(
			row[headerIndices["REQUEST_ID"]],
			initiatedValue,
			closedValue,
			row[headerIndices["DESCRIPTION"]],
			row[headerIndices["INITIATED_BY"]],
			row[headerIndices["PRIORITY"]],
			row[headerIndices["ADDRESS"]],
			row[headerIndices["COMMUNITY"]],
			row[headerIndices["DISTRICT"]],
			row[headerIndices["REQUEST_CATEGORY"]],
			row[headerIndices["RESOLUTION"]],
			row[headerIndices["LATITUDE"]],
			row[headerIndices["LONGITUDE"]],
			row[headerIndices["STATUS"]],
			row[headerIndices["DEPT_RESPONSIBILITY"]],
			row[headerIndices["WORK_ORDER"]],
			row[headerIndices["PROJECT_NAME"]],
			time.Now().UTC().Truncate(time.Millisecond),
		)
		if err != nil {
			return fmt.Errorf("inserting row: %w", err)
		}

		n++
		if n%10000 == 0 {
			log.Println("processed", n, "requests")
		}
	}

	if res, err := tx.Exec(`UPDATE sync_state SET requests_modified=?`, requestsModified); err != nil {
		return fmt.Errorf("updating sync state: %w", err)
	} else if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("getting rows affected: %w", err)
	} else if n == 0 {
		if _, err := tx.Exec(`INSERT INTO sync_state (requests_modified) VALUES (?)`, requestsModified); err != nil {
			return fmt.Errorf("inserting sync state: %w", err)
		}
	}

	var fields io.ReadCloser
	if fieldsFile != "" {
		fields, err = os.Open(fieldsFile)
		if err != nil {
			return fmt.Errorf("opening fields file: %w", err)
		}
	} else {
		const fieldsID = "81703e2cda974ffb8d4ba1f313d18429"
		fields, _, err = download(ctx, fieldsID, time.Time{})
		if err != nil {
			return fmt.Errorf("downloading fields: %w", err)
		}
	}
	defer fields.Close()

	cr = csv.NewReader(fields)
	if _, err := cr.Read(); err != nil {
		return fmt.Errorf("reading fields header: %w", err)
	}

	fieldsYearStmts := make(map[int]*sql.Stmt)
	fieldsYearStmt := func(year int) (*sql.Stmt, error) {
		if stmt, ok := fieldsYearStmts[year]; ok {
			return stmt, nil
		}

		stmt, err := tx.Prepare(fmt.Sprintf(`INSERT INTO request_fields_%d (id, category_id, category, outcome) VALUES (?, ?, ?, ?) ON CONFLICT (id, category_id) DO UPDATE SET category=excluded.category, outcome=excluded.outcome`, year))
		if err != nil {
			return nil, fmt.Errorf("preparing fields statement: %w", err)
		}
		fieldsYearStmts[year] = stmt
		return stmt, nil
	}

	n = 0
	var pe *csv.ParseError
	for {
		row, err := cr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.As(err, &pe) {
			continue
		}
		if err != nil {
			return fmt.Errorf("reading fields row: %w", err)
		}

		requestID, err := strconv.Atoi(row[0])
		if err != nil {
			return fmt.Errorf("parsing request ID: %w", err)
		}

		year, ok := requestYears[requestID]
		if !ok {
			return fmt.Errorf("missing request year: %v", requestID)
		}

		stmt, err := fieldsYearStmt(year)
		if err != nil {
			return fmt.Errorf("getting fields statement: %w", err)
		}

		if _, err = stmt.Exec(row[0], row[1], row[2], row[3]); err != nil {
			return fmt.Errorf("inserting fields row: %w", err)
		}

		n++
		if n%10000 == 0 {
			log.Println("processed", n, "fields")
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

func download(ctx context.Context, id string, lastModified time.Time) (_ io.ReadCloser, modified time.Time, _ error) {
	currentModified, err := func() (time.Time, error) {
		req, err := http.NewRequestWithContext(ctx, "GET", "https://www.arcgis.com/sharing/rest/content/items/"+id+"?f=json", nil)
		if err != nil {
			return time.Time{}, fmt.Errorf("creating request: %w", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return time.Time{}, fmt.Errorf("executing request: %w", err)
		}
		defer resp.Body.Close()

		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return time.Time{}, fmt.Errorf("reading body: %w", err)
		}

		if resp.StatusCode/100 != 2 {
			return time.Time{}, fmt.Errorf("unexpected status code: %d -- %v", resp.StatusCode, string(b))
		}

		var body struct {
			Modified int64 `json:"modified"`
		}
		if err := json.Unmarshal(b, &body); err != nil {
			return time.Time{}, fmt.Errorf("unmarshaling body: %w", err)
		}

		return time.Unix(0, body.Modified*int64(time.Millisecond)), nil
	}()
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("getting modified time: %w", err)
	}
	if !currentModified.After(lastModified) {
		return nil, time.Time{}, nil
	}

	log.Println("downloading", id, "last modified", lastModified, "current modified", currentModified)

	deadline := time.Now().Add(10 * time.Minute)

	var resultURL string
	for time.Now().Before(deadline) {
		u, body, err := func() (string, []byte, error) {
			req, err := http.NewRequestWithContext(ctx, "GET", "https://hub.arcgis.com/api/download/v1/items/"+id+"/csv?redirect=false&layers=0", nil)
			if err != nil {
				return "", nil, fmt.Errorf("creating request: %w", err)
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return "", nil, fmt.Errorf("executing request: %w", err)
			}
			defer resp.Body.Close()

			b, err := io.ReadAll(resp.Body)
			if err != nil {
				return "", nil, fmt.Errorf("reading body: %w", err)
			}

			if resp.StatusCode/100 != 2 {
				return "", nil, fmt.Errorf("unexpected status code: %d -- %v", resp.StatusCode, string(b))
			}

			var body struct {
				ResultURL string `json:"resultUrl"`
			}
			if err := json.Unmarshal(b, &body); err != nil {
				return "", nil, fmt.Errorf("unmarshaling body: %w", err)
			}

			return body.ResultURL, b, nil
		}()
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("downloading data: %w", err)
		}
		if u == "" {
			log.Println("waiting", id, "body", string(body))
			select {
			case <-time.After(10 * time.Second):
			case <-ctx.Done():
				return nil, time.Time{}, ctx.Err()
			}
			continue
		}
		resultURL = u
		break
	}

	log.Println("downloading", id, "from", resultURL)

	req, err := http.NewRequestWithContext(ctx, "GET", resultURL, nil)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("creating request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("executing request: %w", err)
	}
	return resp.Body, currentModified, nil
}

func createYearTable(tx *sql.Tx, year int) error {
	if _, err := tx.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS requests_%d (id TEXT PRIMARY KEY, initiated DATETIME, closed DATETIME, first_observed DATETIME, description TEXT, initiator TEXT, priority TEXT, address TEXT, community TEXT, district TEXT, category TEXT, resolution TEXT, latitude REAL, longitude REAL, status TEXT, department TEXT, work_order TEXT, project_name TEXT)`, year)); err != nil {
		return fmt.Errorf("creating requests table: %w", err)
	}
	if _, err := tx.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS request_fields_%d (id TEXT REFERENCES requests_%d (id), category_id INTEGER, category TEXT, outcome TEXT, PRIMARY KEY (id, category_id))`, year, year)); err != nil {
		return fmt.Errorf("creating fields table: %w", err)
	}
	return nil
}
