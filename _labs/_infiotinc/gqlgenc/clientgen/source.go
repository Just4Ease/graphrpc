package clientgen

import (
	"bytes"
	"fmt"
	"github.com/infiotinc/gqlgenc/config"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/formatter"
	"go/types"
)

type Source struct {
	schema          *ast.Schema
	queryDocument   *ast.QueryDocument
	sourceGenerator *SourceGenerator
	generateConfig  *config.GenerateConfig
}

func NewSource(schema *ast.Schema, queryDocument *ast.QueryDocument, sourceGenerator *SourceGenerator, generateConfig *config.GenerateConfig) *Source {
	return &Source{
		schema:          schema,
		queryDocument:   queryDocument,
		sourceGenerator: sourceGenerator,
		generateConfig:  generateConfig,
	}
}

type TypeTarget struct {
	Type types.Type
	Name string
}

type MapField struct {
	Name string
	Type types.Type
}

type Type struct {
	Name           string
	Path           FieldPath
	Type           types.Type
	UnmarshalTypes map[string]TypeTarget
	RefType        *types.Named
	Consts         []*types.Const

	MapReq []MapField
	MapOpt []MapField
}

func (t Type) IsInputMap() bool {
	return len(t.MapReq) > 0 || len(t.MapOpt) > 0
}

func (s *Source) Fragments() error {
	for _, fragment := range s.queryDocument.Fragments {
		path := NewFieldPath(fragment.Definition.Kind, fragment.Name)
		_ = s.sourceGenerator.namedType(path, func() types.Type {
			responseFields := s.sourceGenerator.NewResponseFields(path, &fragment.SelectionSet)

			typ := s.sourceGenerator.genFromResponseFields(path, responseFields)
			return typ
		})
	}

	return nil
}

func (s *Source) ExtraTypes() error {
	for _, t := range s.sourceGenerator.ccfg.Client.ExtraTypes {
		def := s.sourceGenerator.cfg.Schema.Types[t]

		if def == nil {
			panic("type " + t + " does not exist in schema")
		}

		_ = s.sourceGenerator.namedType(NewFieldPath(def.Kind, def.Name), func() types.Type {
			return s.sourceGenerator.genFromDefinition(def)
		})
	}

	return nil
}

type Operation struct {
	Name                string
	ResponseType        types.Type
	Operation           string
	OperationType       string
	Args                []*Argument
	VariableDefinitions ast.VariableDefinitionList
}

func NewOperation(operation *OperationResponse, queryDocument *ast.QueryDocument, args []*Argument) *Operation {
	return &Operation{
		Name:                operation.Name,
		OperationType:       string(operation.Operation.Operation),
		ResponseType:        operation.Type,
		Operation:           queryString(queryDocument),
		Args:                args,
		VariableDefinitions: operation.Operation.VariableDefinitions,
	}
}

func (s *Source) Operations(queryDocuments []*ast.QueryDocument, operationResponses []*OperationResponse) []*Operation {
	operations := make([]*Operation, 0, len(s.queryDocument.Operations))

	queryDocumentsMap := queryDocumentMapByOperationName(queryDocuments)
	operationArgsMap := s.operationArgsMapByOperationName()
	for _, operation := range operationResponses {
		queryDocument := queryDocumentsMap[operation.Name]
		args := operationArgsMap[operation.Name]
		operations = append(operations, NewOperation(operation, queryDocument, args))
	}

	return operations
}

func (s *Source) operationArgsMapByOperationName() map[string][]*Argument {
	operationArgsMap := make(map[string][]*Argument)
	for _, operation := range s.queryDocument.Operations {
		operationArgsMap[operation.Name] = s.sourceGenerator.OperationArguments(operation.VariableDefinitions)
	}

	return operationArgsMap
}

func queryDocumentMapByOperationName(queryDocuments []*ast.QueryDocument) map[string]*ast.QueryDocument {
	queryDocumentMap := make(map[string]*ast.QueryDocument)
	for _, queryDocument := range queryDocuments {
		operation := queryDocument.Operations[0]
		queryDocumentMap[operation.Name] = queryDocument
	}

	return queryDocumentMap
}

func queryString(queryDocument *ast.QueryDocument) string {
	var buf bytes.Buffer
	astFormatter := formatter.NewFormatter(&buf)
	astFormatter.FormatQueryDocument(queryDocument)

	return buf.String()
}

type OperationResponse struct {
	Operation *ast.OperationDefinition
	Name      string
	Type      types.Type
}

const OperationKind ast.DefinitionKind = "OPERATION"

func (s *Source) OperationResponses() ([]*OperationResponse, error) {
	operationResponses := make([]*OperationResponse, 0, len(s.queryDocument.Operations))
	for _, operationResponse := range s.queryDocument.Operations {
		name := getResponseStructName(operationResponse, s.generateConfig)

		path := NewFieldPath(OperationKind, name)
		namedType := s.sourceGenerator.namedType(path, func() types.Type {
			responseFields := s.sourceGenerator.NewResponseFields(path, &operationResponse.SelectionSet)

			typ := s.sourceGenerator.genFromResponseFields(path, responseFields)
			return typ
		})

		operationResponses = append(operationResponses, &OperationResponse{
			Operation: operationResponse,
			Name:      name,
			Type:      namedType,
		})
	}

	return operationResponses, nil
}

func getResponseStructName(operation *ast.OperationDefinition, generateConfig *config.GenerateConfig) string {
	name := operation.Name
	if generateConfig != nil {
		if generateConfig.Prefix != nil {
			if operation.Operation == ast.Subscription {
				name = fmt.Sprintf("%s%s", generateConfig.Prefix.Subscription, name)
			}

			if operation.Operation == ast.Mutation {
				name = fmt.Sprintf("%s%s", generateConfig.Prefix.Mutation, name)
			}

			if operation.Operation == ast.Query {
				name = fmt.Sprintf("%s%s", generateConfig.Prefix.Query, name)
			}
		}

		if generateConfig.Suffix != nil {
			if operation.Operation == ast.Subscription {
				name = fmt.Sprintf("%s%s", name, generateConfig.Suffix.Subscription)
			}

			if operation.Operation == ast.Mutation {
				name = fmt.Sprintf("%s%s", name, generateConfig.Suffix.Mutation)
			}

			if operation.Operation == ast.Query {
				name = fmt.Sprintf("%s%s", name, generateConfig.Suffix.Query)
			}
		}
	}

	return name
}
