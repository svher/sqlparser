/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqlparser

import (
	"testing"

	"github.com/xwb1989/sqlparser/dependency/querypb"
)

// BenchmarkNormalize tests the performance of normalizing a query
func BenchmarkNormalize(b *testing.B) {
	sql := "select * from user where id = 5 and name = 'john' and age = 30"
	stmt, err := Parse(sql)
	if err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bindVars := make(map[string]*querypb.BindVariable)
		Normalize(stmt, bindVars, "bv")
	}
}

// BenchmarkPreview tests the performance of query type detection
func BenchmarkPreview(b *testing.B) {
	queries := []string{
		"SELECT * FROM users WHERE id = 1",
		"INSERT INTO users (name, email) VALUES ('test', 'test@example.com')",
		"UPDATE users SET name = 'updated' WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
		"CREATE TABLE test (id INT PRIMARY KEY)",
		"BEGIN",
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, q := range queries {
			_ = Preview(q)
		}
	}
}

// BenchmarkTokenizer tests tokenizer performance
func BenchmarkTokenizer(b *testing.B) {
	sql := "SELECT id, name, email FROM users WHERE status = 'active' AND created_at > '2023-01-01'"
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tokenizer := NewStringTokenizer(sql)
		for {
			typ, _ := tokenizer.Scan()
			if typ == 0 {
				break
			}
		}
	}
}

// BenchmarkParseInsert tests INSERT statement parsing
func BenchmarkParseInsert(b *testing.B) {
	sql := "INSERT INTO users (id, name, email, status) VALUES (1, 'john', 'john@example.com', 'active')"
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseUpdate tests UPDATE statement parsing
func BenchmarkParseUpdate(b *testing.B) {
	sql := "UPDATE users SET name = 'john', email = 'john@example.com' WHERE id = 1 AND status = 'active'"
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParseJoin tests complex JOIN parsing
func BenchmarkParseJoin(b *testing.B) {
	sql := "SELECT u.id, u.name, p.title FROM users u INNER JOIN posts p ON u.id = p.user_id WHERE u.status = 'active' ORDER BY u.created_at DESC"
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStringify tests converting AST back to string
func BenchmarkStringify(b *testing.B) {
	sql := "SELECT id, name, email FROM users WHERE status = 'active' ORDER BY created_at DESC LIMIT 10"
	stmt, err := Parse(sql)
	if err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = String(stmt, false)
	}
}

// BenchmarkIsDML tests IsDML function
func BenchmarkIsDML(b *testing.B) {
	queries := []string{
		"SELECT * FROM users",
		"INSERT INTO users VALUES (1, 'test')",
		"UPDATE users SET name = 'test'",
		"DELETE FROM users WHERE id = 1",
		"CREATE TABLE test (id INT)",
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, q := range queries {
			_ = IsDML(q)
		}
	}
}
