package maildatabase

import (
	"database/sql"
	"log"
	"time"

	"github.com/mattn/go-sqlite3"
)

// Email record struct in the database
type EmailEntry struct {
	Id          int
	Email       string
	ConfirmedAt *time.Time
	OptOut      bool
}

func TryCreateDatabase(db *sql.DB) {
	_, err := db.Exec(`
		CREATE TABLE emails (
			id			INTEGER PRIMARY KEY,
			email		TEXT UNIQUE,
			confirmed_at INTEGER,
			opt_out INTEGER, 
		);
	`)
	if err != nil {
		// Taking the err and casting it to sqlite3.Error type
		if sqlError, ok := err.(sqlite3.Error); ok {
			// With the sqlite3.Error type we can use
			// the .Code.
			// code 1 == Table already exists
			if sqlError.Code != 1 {
				log.Fatal(sqlError)
			}
		} else {
			log.Fatal(err)
		}
	}
}

func emailEntryFromRow(row *sql.Rows) (*EmailEntry, error) {
	// Creating variables for each fields in the struct
	var id int
	var email string
	var confirmedAt int64
	var optOut bool

	// Reads values in database and copies to destinations
	err := row.Scan(&id, &email, &confirmedAt, &optOut)

	// Check if no error, and returns if the operation goes successfully
	if err != nil {
		log.Println(err)
		return nil, err
	}

	// Convert from int64 time to an appropriate time format
	time := time.Unix(confirmedAt, 0)

	return &EmailEntry{Id: id, Email: email, ConfirmedAt: &time, OptOut: optOut}, nil
}

func CreateEmail(db *sql.DB, email string) error {
	// the ? refers to the provided value of email
	_, err := db.Exec(`INSERT INTO
		emails(email,confirmed_at, opt_out)
		VALUES(?, 0, false)`, email)
	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

// Returns and email related data from the database
func GetEmail(db *sql.DB, email string) (*EmailEntry, error) {
	rows, err := db.Query(
		`
			SELECT id, email, confirmed_at, opt_out
			FROM emails
			WHERE email = ?
		`, email,
	)

	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		return emailEntryFromRow(rows)
	}

	return nil, nil
}

func UpdateEmail(db *sql.DB, entry EmailEntry) error {
	// Convert the time to an integer format
	time := entry.ConfirmedAt.Unix()

	_, err := db.Query(
		`
			INSERT INTO
			emails(email, confirmed_at, opt_out)
			VALUES(?,?,?)
			ON CONFLICT(email) DO UPDATE SET
				confirmed_at=?
				opt_out=?

		`, entry.Email, time, entry.OptOut, time, entry.OptOut,
	)

	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func DeleteEmail(db *sql.DB, email string) error {
	_, err := db.Exec(
		`	UPDATE emails
			SET opt_out=true
			WHERE email=?
		`, email,
	)

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

type GetEmailBatchQueryParams struct {
	Page  int
	Count int
}

func GetEmailBatch(db *sql.DB, params GetEmailBatchQueryParams) ([]EmailEntry, error) {
	var empty []EmailEntry

	rows, err := db.Query(
		`
			SELECT id, email, confirmed_at, opt_out
			FROM emails
			WHERE opt_out=false
			ORDER BY id ASC
			LIMIT ? OFFSET ?

		`, params.Count, (params.Page-1)*params.Count,
	)

	if err != nil {
		log.Println(err)
		return empty, err
	}

	defer rows.Close()

	emails := make([]EmailEntry, 0, params.Count)

	for rows.Next() {
		email, err := emailEntryFromRow(rows)
		if err != nil {
			return nil, err
		}

		emails = append(emails, *email)
	}

	return emails, nil
}
