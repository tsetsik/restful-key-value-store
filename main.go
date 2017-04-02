package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type myHandler struct {
	filter *regexp.Regexp
	store  store
}

type store struct {
	db          map[string]interface{}
	expirations map[string]interface{}
	errors      chan string
}

const (
	port              int = 8000
	defaultExpiration     = time.Hour * 2
)

func (s *store) handlePath(w http.ResponseWriter, r *http.Request, path string) {
	p := strings.Replace(path, "/db", "", -1)
	result := deleteEmpty(strings.Split(p, "/"))

	if r.Method == "GET" {
		s.getKey(w, r, result)
	} else if r.Method == "PUT" {
		s.putKey(w, r, result)
	} else if r.Method == "DELETE" {
		s.deleteKey(w, r, result)
	} else {
		http.NotFound(w, r)
	}
}

func deleteEmpty(s []string) []string {
	var r []string
	for _, str := range s {
		if str != "" {
			r = append(r, str)
		}
	}
	return r
}

func isExpiredKey(key map[string]interface{}) bool {
	t_exp := key["created_at"].(time.Time).Add(key["expiration"].(time.Duration))
	diff := t_exp.Sub(time.Now())
	return diff < 0
}

func findKey(m map[string]interface{}, key []string) interface{} {
	var result interface{} = false

	for index, k := range key {
		if value, ok := m[k]; ok {
			next_keys := key[(index + 1):]
			if (len(key) - 1) == index {
				result = value
			} else {
				switch v := value.(type) {
				case map[string]interface{}:
					return findKey(v, next_keys)
				case []interface{}:
					if i, _ := strconv.Atoi(next_keys[0]); i > 0 && i <= len(v) {
						result = v[i-1]
					}
				}
			}
		}
	}

	return result
}

func (s *store) removeKey(key string) bool {
	_, db_ok := s.db[key]
	_, exp_ok := s.expirations[key]

	if db_ok && exp_ok {
		delete(s.db, key)
		delete(s.expirations, key)
		return true
	}

	return false
}

func (s *store) reportError(code int, message string) map[string]interface{} {
	s.errors <- message
	return map[string]interface{}{"error": true, "code": code, "message": message}
}

func (s *store) reportSuccess(body interface{}) map[string]interface{} {
	return map[string]interface{}{"error": false, "body": body}
}

func parseExpiration(exp string) time.Duration {
	dr, d_err := time.ParseDuration(exp)
	if d_err != nil {
		dr = defaultExpiration
	}

	return dr
}

func (s *store) removeExpiredKeys() {
	var chk sync.WaitGroup
	expired := []string{}

	for key, item := range s.expirations {
		chk.Add(1)
		go func(k string, i map[string]interface{}) {
			if isExpiredKey(i) {
				expired = append(expired, k)
			}
			chk.Done()
		}(key, item.(map[string]interface{}))
	}

	chk.Wait()
	for _, exp_key := range expired {
		s.removeKey(exp_key)
	}
}

func (s *store) getKey(w http.ResponseWriter, r *http.Request, path []string) {
	var key interface{} = ""
	err := "Not found path"

	exp_key, key_ok := s.expirations[path[0]]
	if key_ok && !isExpiredKey(exp_key.(map[string]interface{})) {
		key = findKey(s.db, path)
		if key != false {
			err = ""
		}
	}

	if err == "" {
		json.NewEncoder(w).Encode(s.reportSuccess(key))
	} else {
		json.NewEncoder(w).Encode(s.reportError(2, err))
	}
}

func (s *store) putKey(w http.ResponseWriter, r *http.Request, path []string) {
	r.ParseForm()
	result := map[string]interface{}{}

	data := r.FormValue("data")
	expiration := parseExpiration(r.FormValue("expiration"))

	ji := map[string]interface{}{}
	err := json.Unmarshal([]byte(data), &ji)

	if err != nil || len(ji) == 0 {
		result = s.reportError(1, "Invalid json format. Please provide valid json")
	} else {
		key := path[0]
		s.db[key] = ji
		s.expirations[key] = map[string]interface{}{"expiration": expiration, "created_at": time.Now()}

		result = s.reportSuccess(fmt.Sprintf("Success on adding key %s", key))
	}

	json.NewEncoder(w).Encode(result)
}

func (s *store) deleteKey(w http.ResponseWriter, r *http.Request, path []string) {
	var err string = "Invalid delete key"

	if len(path) == 1 {
		if s.removeKey(path[0]) {
			err = ""
		} else {
			err = "Key not found"
		}
	}

	if err == "" {
		msg := fmt.Sprintf("Success on removing the key %s", path[0])
		json.NewEncoder(w).Encode(s.reportSuccess(msg))
	} else {
		json.NewEncoder(w).Encode(s.reportError(3, err))
	}
}

func (m *myHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	str := m.filter.FindString(r.URL.String())

	if str != "" {
		m.store.handlePath(w, r, str)
		m.store.removeExpiredKeys()

		return
	}

	http.NotFound(w, r)
}

func logErrors(ch chan string) {
	fErr, _ := os.OpenFile("error.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	for {
		fErr.WriteString(<-ch)
	}
}

func main() {
	s := store{errors: make(chan string), db: map[string]interface{}{}, expirations: map[string]interface{}{}}
	regxp := regexp.MustCompile("/db/(.*)$")

	server := http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		Handler:      &myHandler{filter: regxp, store: s},
	}

	fmt.Printf("Now you can visit http://127.0.0.1:%d \n", port)
	go logErrors(s.errors)
	log.Fatal(server.ListenAndServe())
}
