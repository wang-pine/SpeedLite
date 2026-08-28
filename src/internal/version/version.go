// Package version 提供构建期注入的版本号（由 makeVersion.sh 通过 -ldflags 注入）。
package version

// Version 应用版本号，构建时通过 -ldflags "-X speedTest/internal/version.Version=1.1.0" 注入。
// 默认值为开发版占位。
var Version = "dev"