package version

const (
	version         = "0.8.0"
	flockerHubURL   = "https://data.flockerhub.clusterhq.com"
	zfsReferenceURL = "https://clusterhq.com/zfs-tools/"
)

var (
	gitCommit = ""
	buildTime = ""
)

// Version returns the version string
func Version() string {
	return version
}

// CommitID returns the git commit id (git SHA)
func CommitID() string {
	return gitCommit
}

// FlockerHubURL returns the FlockerHub URL (dp-server)
func FlockerHubURL() string {
	return flockerHubURL
}

// ZFSReferenceURL returns the ZFS Reference docs URL
func ZFSReferenceURL() string {
	return zfsReferenceURL
}

// BuildTime returns the fli build time
func BuildTime() string {
	return buildTime
}
