package types

type Participant struct {
	AccessRule Access
	EntryPoint string
	Name       string
	Connection struct {
		Address string
		Host    string
		Port    string
		Token   string
		// TODO: Do maybe a tls conf here too. Still WIP :p
	}
}

type SubjectPermission struct {
	Allow []string `json:"allow,omitempty"`
	Deny  []string `json:"deny,omitempty"`
}

type Access struct {
	Mode         AccessMode
	Participants []Participant
}

type AccessMode string

const (
	Grant AccessMode = "GRANT"
	Deny  AccessMode = "DENY"
)

func (a AccessMode) IsValid() bool {
	switch a {
	case Grant, Deny:
		return true
	default:
		return false
	}
}

func (a AccessMode) String() string {
	return string(a)
}
