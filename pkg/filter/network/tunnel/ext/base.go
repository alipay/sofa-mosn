package ext

type ServerLister interface {
	List(string) (chan []string)
}

var serverListers = map[string]ServerLister{}

func RegisterServerLister(name string, lister ServerLister) {
	serverListers[name] = lister
}

func GetServerLister(name string) ServerLister {
	return serverListers[name]
}


// ConnectionValidator
type ConnectionValidator interface {
	Validate(credential string, host string, cluster string) bool
}


func RegisterConnectionValidator(name string, validator ConnectionValidator) {
	connectionValidators[name] = validator
}

func GetConnectionValidator(name string) ConnectionValidator {
	return connectionValidators[name]
}

var connectionValidators = map[string]ConnectionValidator{}


// ConnectionCredentialGetter
type ConnectionCredentialGetter func(cluster string) string

func RegisterConnectionCredentialGetter(name string, getter ConnectionCredentialGetter) {
	connectionCredentialGetters[name] = getter
}

func GetConnectionCredentialGetter(name string) ConnectionCredentialGetter {
	return connectionCredentialGetters[name]
}

var connectionCredentialGetters = map[string]ConnectionCredentialGetter{}
