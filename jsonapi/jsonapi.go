package jsonapi

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"maillinglist/maildatabase"
	"net/http"
)

func setJsonHeader(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
}

func fromJson[T any](body io.Reader, target T) {
	buffer := new(bytes.Buffer)
	buffer.ReadFrom(body)
	json.Unmarshal(buffer.Bytes(), &target)
}

func returnJsonData[T any](w http.ResponseWriter, withData func() (T, error)) {
	setJsonHeader(w)

	data, serverError := withData()

	if serverError != nil {
		w.WriteHeader(500)
		serverErrorJson, err := json.Marshal(&serverError)

		if err != nil {
			log.Println(err)
			return
		}
		w.Write(serverErrorJson)
		return
	}

	dataJson, err := json.Marshal(&data)
	if err != nil {
		log.Print(err)
		w.WriteHeader(500)
		return
	}

	w.Write(dataJson)
}

func returnErr(w http.ResponseWriter, err error, code int) {
	returnJsonData(w, func() (interface{}, error) {
		errorMessage := struct {
			Err string
		}{
			Err: err.Error(),
		}
		w.WriteHeader(code)
		return errorMessage, nil
	})
}

func CreateEmail(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			return
		}

		entry := maildatabase.EmailEntry{}
		fromJson(r.Body, &entry)

		if err := maildatabase.CreateEmail(db, entry.Email); err != nil {
			returnErr(w, err, 400)
			return
		}

		returnJsonData(w, func() (interface{}, error) {
			log.Printf("Json CreateEmail: %v\n", entry.Email)
			return maildatabase.GetEmail(db, entry.Email)
		})
	})
}

func GetEmail(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			return
		}

		entry := maildatabase.EmailEntry{}
		fromJson(r.Body, &entry)

		returnJsonData(w, func() (interface{}, error) {
			log.Printf("Json GetEmail: %v\n", entry.Email)
			return maildatabase.GetEmail(db, entry.Email)
		})
	})
}

func UpdateEmail(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "PUT" {
			return
		}

		entry := maildatabase.EmailEntry{}
		fromJson(r.Body, &entry)

		if err := maildatabase.UpdateEmail(db, entry); err != nil {
			returnErr(w, err, 400)
			return
		}

		returnJsonData(w, func() (interface{}, error) {
			log.Printf("Json UpdateEmail: %v\n", entry.Email)
			return maildatabase.GetEmail(db, entry.Email)
		})
	})
}

func DeleteEmail(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			return
		}

		entry := maildatabase.EmailEntry{}
		fromJson(r.Body, &entry)

		if err := maildatabase.DeleteEmail(db, entry.Email); err != nil {
			returnErr(w, err, 400)
			return
		}

		returnJsonData(w, func() (interface{}, error) {
			log.Printf("Json DeleteEmail: %v\n", entry.Email)
			return maildatabase.GetEmail(db, entry.Email)
		})
	})
}

func GetEmailBatch(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			return
		}

		queryOptions := maildatabase.GetEmailBatchQueryParams{}
		fromJson(r.Body, &queryOptions)

		if queryOptions.Count <= 0 || queryOptions.Page <= 0 {
			returnErr(w, errors.New("page and Count fields are required and must be > 0"), 400)
			return
		}

		returnJsonData(w, func() (interface{}, error) {
			log.Printf("Json GetEmailBatch: %v\n", queryOptions)
			return maildatabase.GetEmailBatch(db, queryOptions)
		})
	})
}

func Serve(db *sql.DB, address string) {
	http.Handle("/email/create", CreateEmail(db))
	http.Handle("/email/get", GetEmail(db))
	http.Handle("/email/get_batch", GetEmailBatch(db))
	http.Handle("/email/update", UpdateEmail(db))
	http.Handle("/email/delete", DeleteEmail(db))
	err := http.ListenAndServe(address, nil)
	if err != nil {
		log.Fatalf("Json server error: %v", err)
	}
}
