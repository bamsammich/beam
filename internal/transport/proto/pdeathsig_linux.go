package proto

import "syscall"

// setPdeathsig sets Pdeathsig so the child dies if the parent crashes.
// Only available on Linux.
func setPdeathsig(attr *syscall.SysProcAttr) {
	attr.Pdeathsig = syscall.SIGTERM
}
