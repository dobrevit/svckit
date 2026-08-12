// Package audit emits audit events onto the event bus.
//
// Audit records are compliance artefacts rather than diagnostics: they carry
// who did what to which resource, and they are published as signed events so
// a consumer can verify they were not forged in transit.
package audit
