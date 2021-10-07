package servergen

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

var defaultQuerySchema = `
type Query {
  todos: [Todo!]!
}
`

var defaultMutationSchema = `
type Mutation {
  createTodo(payload: CreateTodoPayload!): Todo!
}
`

var defaultSubscriptionSchema = `
type Subscription {
	latestTodo(userId: String!): Todo!
}
`

var defaultTypesAndInputSchema = `
directive @goField(forceResolver: Boolean, name: String) on INPUT_FIELD_DEFINITION
    | FIELD_DEFINITION

type Todo {
  id: ID!
  text: String!
  done: Boolean!
  user: User! @goField(forceResolver: true)
}

type User {
  id: ID!
  name: String!
}

input CreateTodoPayload {
  text: String!
  userId: String!
}

`

func PrepareSchema(schemaPath string) error {
	_, err := os.Stat(schemaPath)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(schemaPath), 0755); err != nil {
			return fmt.Errorf("unable to create schema dir: " + err.Error())
		}
	}

	schemaFiles := make([]string, 0)
	if err := filepath.Walk(filepath.Dir(schemaPath), func(path string, f os.FileInfo, err error) error {
		if strings.Contains(filepath.Ext(path), "graphql") {
			schemaFiles = append(schemaFiles, path)
			return nil
		}
		return nil
	}); err != nil {
		return fmt.Errorf("unable to prepate schema schemas in directory: " + err.Error())
	}

	if len(schemaFiles) >= 1 {
		return nil
	}

	modelsFile := filepath.Clean(fmt.Sprintf("%s/models.graphql", filepath.Dir(schemaPath)))
	if err = ioutil.WriteFile(modelsFile, []byte(defaultTypesAndInputSchema), 0644); err != nil {
		return fmt.Errorf("unable to write models schema file: " + err.Error())
	}

	queriesFile := filepath.Clean(fmt.Sprintf("%s/queries.graphql", filepath.Dir(schemaPath)))
	if err = ioutil.WriteFile(queriesFile, []byte(defaultQuerySchema), 0644); err != nil {
		return fmt.Errorf("unable to write queries schema file: " + err.Error())
	}

	mutationsFile := filepath.Clean(fmt.Sprintf("%s/mutations.graphql", filepath.Dir(schemaPath)))
	if err = ioutil.WriteFile(mutationsFile, []byte(defaultMutationSchema), 0644); err != nil {
		return fmt.Errorf("unable to write mutations schema file: " + err.Error())
	}

	subscriptionsFile := filepath.Clean(fmt.Sprintf("%s/subscriptions.graphql", filepath.Dir(schemaPath)))
	if err = ioutil.WriteFile(subscriptionsFile, []byte(defaultSubscriptionSchema), 0644); err != nil {
		return fmt.Errorf("unable to write subscriptions schema file: " + err.Error())
	}
	return nil
}
