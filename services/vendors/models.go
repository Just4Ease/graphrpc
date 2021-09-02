// Code generated by github.com/99designs/gqlgen, DO NOT EDIT.

package vendorService

import (
	"fmt"
	"io"
	"strconv"
)

type Node interface {
	IsNode()
}

type Status interface {
	IsStatus()
}

type CreateVendorInput struct {
	Name         string `json:"name"`
	ContactEmail string `json:"contactEmail"`
	ContactPhone string `json:"contactPhone"`
	Address      string `json:"address"`
	Iso2         string `json:"iso2"`
	City         string `json:"city"`
	State        string `json:"state"`
	StateCode    string `json:"stateCode"`
	Country      string `json:"country"`
	UserID       string `json:"userId"`
}

type Result struct {
	Success    bool        `json:"success"`
	Message    string      `json:"message"`
	Token      string      `json:"token"`
	ResultType ResultType  `json:"resultType"`
	Data       interface{} `json:"data,omitempty"`
}

type UpdateVendorInput struct {
	ID           string  `json:"id"`
	Name         *string `json:"name,omitempty"`
	ContactEmail *string `json:"contactEmail,omitempty"`
	ContactPhone *string `json:"contactPhone,omitempty"`
	Address      *string `json:"address,omitempty"`
	Iso2         *string `json:"iso2,omitempty"`
	City         *string `json:"city,omitempty"`
	State        *string `json:"state,omitempty"`
	Country      *string `json:"country,omitempty"`
	UserID       *string `json:"userId,omitempty"`
}

type Vendor struct {
	ID           string       `json:"id"`
	Name         string       `json:"name"`
	ContactEmail string       `json:"contactEmail"`
	ContactPhone string       `json:"contactPhone"`
	Address      string       `json:"address"`
	Country      string       `json:"country"`
	Iso2         string       `json:"iso2"`
	State        string       `json:"state"`
	StateCode    string       `json:"stateCode"`
	City         string       `json:"city"`
	Status       VendorStatus `json:"status"`
	UserID       string       `json:"userId"`
	TimeCreated  int          `json:"timeCreated"`
	TimeUpdated  int          `json:"timeUpdated"`
}

func (Vendor) IsNode() {}

type VendorFilters struct {
	CursorID     string        `json:"cursorId"`
	PreviousPage bool          `json:"previousPage"`
	UserID       string        `json:"userId"`
	Iso2         string        `json:"iso2"`
	StateCode    string        `json:"stateCode"`
	Status       *VendorStatus `json:"status,omitempty"`
	Limit        int           `json:"limit"`
}

type VendorList struct {
	Count int       `json:"count"`
	Data  []*Vendor `json:"data,omitempty"`
}

type ResultType string

const (
	ResultTypeOk        ResultType = "OK"
	ResultTypeNotOk     ResultType = "NOT_OK"
	ResultTypeCreated   ResultType = "CREATED"
	ResultTypeDuplicate ResultType = "DUPLICATE"
	ResultTypeUpdated   ResultType = "UPDATED"
	ResultTypeDeleted   ResultType = "DELETED"
	ResultTypeNotFound  ResultType = "NOT_FOUND"
)

var AllResultType = []ResultType{
	ResultTypeOk,
	ResultTypeNotOk,
	ResultTypeCreated,
	ResultTypeDuplicate,
	ResultTypeUpdated,
	ResultTypeDeleted,
	ResultTypeNotFound,
}

func (e ResultType) IsValid() bool {
	switch e {
	case ResultTypeOk, ResultTypeNotOk, ResultTypeCreated, ResultTypeDuplicate, ResultTypeUpdated, ResultTypeDeleted, ResultTypeNotFound:
		return true
	}
	return false
}

func (e ResultType) String() string {
	return string(e)
}

func (e *ResultType) UnmarshalGQL(v interface{}) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("enums must be strings")
	}

	*e = ResultType(str)
	if !e.IsValid() {
		return fmt.Errorf("%s is not a valid ResultType", str)
	}
	return nil
}

func (e ResultType) MarshalGQL(w io.Writer) {
	fmt.Fprint(w, strconv.Quote(e.String()))
}

type VendorStatus string

const (
	VendorStatusActivated   VendorStatus = "activated"
	VendorStatusDeactivated VendorStatus = "deactivated"
)

var AllVendorStatus = []VendorStatus{
	VendorStatusActivated,
	VendorStatusDeactivated,
}

func (e VendorStatus) IsValid() bool {
	switch e {
	case VendorStatusActivated, VendorStatusDeactivated:
		return true
	}
	return false
}

func (e VendorStatus) String() string {
	return string(e)
}

func (e *VendorStatus) UnmarshalGQL(v interface{}) error {
	str, ok := v.(string)
	if !ok {
		return fmt.Errorf("enums must be strings")
	}

	*e = VendorStatus(str)
	if !e.IsValid() {
		return fmt.Errorf("%s is not a valid VendorStatus", str)
	}
	return nil
}

func (e VendorStatus) MarshalGQL(w io.Writer) {
	fmt.Fprint(w, strconv.Quote(e.String()))
}
