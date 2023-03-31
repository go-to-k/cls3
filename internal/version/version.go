package version

import "runtime/debug"

var Version = ""
var Revision = ""

func IsDebug() bool {
	return Version == "" || Revision != ""
}

func GetVersion() string {
	if Version != "" && Revision != "" {
		return Version + "-" + Revision
	}
	if Version != "" {
		return Version
	}

	i, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	return i.Main.Version
}
