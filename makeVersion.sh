#!/usr/bin/env bash
# ============================================================
# makeVersion.sh — speedTest 版本化多平台构建打包脚本
#
# 用法：
#   ./makeVersion.sh              按 config/version.ini 构建全部平台
#   ./makeVersion.sh <版本号>      临时覆盖版本号（如 1.2.3）
#   ./makeVersion.sh -h           帮助
#
# 读取 config/version.ini：
#   [version]  major/minor/patch        → 应用版本号
#   [app]      name                     → 产物名前缀
#   [platforms]  平台名=GOOS/GOARCH 后缀 → 目标平台矩阵
#
# 产物输出：
#   dest/<平台名>/speedtest-server[-<版本>][.exe]
#   dest/<平台名>/speedctl[-<版本>][.exe]
#   dest/<平台名>/SHA256SUMS            校验和
#   dest/<平台名>/speedtest-<版本>-<平台名>.tar.gz   发行包（linux/darwin）
#   dest/<平台名>/speedtest-<版本>-<平台名>.zip      发行包（windows）
# ============================================================
set -euo pipefail

# ---------- 定位脚本与工程根（脚本位于项目根目录） ----------
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$ROOT_DIR/src"
CONFIG_FILE="$ROOT_DIR/config/version.ini"
DEST_DIR="$ROOT_DIR/dest"

# ---------- Go 工具链探测（兼容自定义安装路径） ----------
if ! command -v go >/dev/null 2>&1; then
  for cand in "$HOME/.tools/go/bin/go" "$HOME/workSpace/.tools/go/bin/go" \
              "/usr/local/go/bin/go" "/usr/lib/go/bin/go" "/opt/go/bin/go"; do
    if [ -x "$cand" ]; then
      export PATH="$(dirname "$cand"):$PATH"
      break
    fi
  done
fi
if ! command -v go >/dev/null 2>&1; then
  echo "[error] 未找到 go 命令，请先安装 Go 或将其加入 PATH" >&2
  exit 1
fi

# ---------- 简易 INI 解析（无外部依赖） ----------
# 读取 [section] 下 key = value，去掉注释/空白
ini_get() {
  local section="$1" key="$2" val
  val=$(awk -F'=' -v sec="$section" -v key="$key" '
    BEGIN{found=0; keymatch=0}
    /^\[/{ found = ($0 ~ ("\\[" sec "\\]")) }
    found && $0 ~ ("^[[:space:]]*" key "[[:space:]]*=") {
      sub(/^[^=]*=[[:space:]]*/, ""); sub(/[[:space:]]*;.*$/, ""); sub(/[[:space:]]*$/, "");
      print; exit
    }' "$CONFIG_FILE")
  echo "$val"
}

# ---------- 版本 ----------
MAJOR=$(ini_get version major); MINOR=$(ini_get version minor); PATCH=$(ini_get version patch)
if [ -z "${MAJOR:-}" ] || [ -z "${MINOR:-}" ] || [ -z "${PATCH:-}" ]; then
  echo "[error] config/version.ini 缺少 [version] major/minor/patch" >&2
  exit 1
fi
VERSION="$MAJOR.$MINOR.$PATCH"
[ $# -ge 1 ] && [ "$1" != "-h" ] && VERSION="$1"   # 命令行覆盖

APP_NAME=$(ini_get app name); APP_NAME="${APP_NAME:-speedtest}"

# ---------- 构建函数 ----------
# Go 缓存可写性探测：GOCACHE/GOMODCACHE 默认在 $HOME 下，沙箱/受限环境可能只读，
# 不可写则回退到项目内 .cache/（gitignored），保证构建稳定。
ensure_go_cache() {
  local cand="${GOCACHE:-}"
  [ -z "$cand" ] && cand="$HOME/.cache/go-build"
  if ! mkdir -p "$cand" 2>/dev/null || ! touch "$cand/.wtest" 2>/dev/null; then
    cand="$ROOT_DIR/.cache/go-build"
    mkdir -p "$cand" 2>/dev/null || true
  fi
  GOCACHE="$cand"
  echo "$cand"
}

ensure_go_modcache() {
  local cand="${GOMODCACHE:-}"
  [ -z "$cand" ] && cand="${GOPATH:-$HOME/go}/pkg/mod"
  if ! mkdir -p "$cand" 2>/dev/null || ! touch "$cand/.wtest" 2>/dev/null; then
    cand="$ROOT_DIR/.cache/pkg/mod"
    mkdir -p "$cand" 2>/dev/null || true
  fi
  GOMODCACHE="$cand"
  echo "$cand"
}

build_one() {
  local plat="$1" goos="$2" goarch="$3" suffix="${4:-}"
  local outdir="$DEST_DIR/$plat"
  mkdir -p "$outdir"

  echo ""
  echo "==> [$plat] $goos/$goarch 编译中…"

  local goCache goModCache goPath
  goCache="$(ensure_go_cache)"
  goModCache="$(ensure_go_modcache)"
  goPath="$(dirname "$goModCache")"   # 把 mod 缓存同级的 gopath 一并定位
  local envs=(GOOS="$goos" GOARCH="$goarch" CGO_ENABLED=0 GOCACHE="$goCache" \
              GOMODCACHE="$goModCache" \
              GOFLAGS="-mod=mod -buildvcs=false" GOPROXY="https://goproxy.cn,direct")
  [ -n "${GOPATH:-}" ] && envs+=("GOPATH=$GOPATH")

  local srv="$APP_NAME-server${suffix}"
  local cli="speedctl${suffix}"
  local srv_out="$outdir/$srv"
  local cli_out="$outdir/$cli"

  ( cd "$SRC_DIR" && env "${envs[@]}" go build -trimpath -ldflags="-s -w -X speedTest/internal/version.Version=$VERSION" \
      -o "$srv_out" ./cmd/speedtest-server )
  ( cd "$SRC_DIR" && env "${envs[@]}" go build -trimpath -ldflags="-s -w -X speedTest/internal/version.Version=$VERSION" \
      -o "$cli_out" ./cmd/speedctl )

  # 版本文件
  echo "$VERSION" > "$outdir/VERSION"
  # 校验和
  ( cd "$outdir" && sha256sum $srv $cli > SHA256SUMS )

  # 发行包
  local pkgbase="${APP_NAME}-${VERSION}-${plat}"
  if [ "$goos" = "windows" ]; then
    ( cd "$outdir" && zip -q -9 "${pkgbase}.zip" "$srv" "$cli" "SHA256SUMS" "VERSION" 2>/dev/null \
      || zip -q "${pkgbase}.zip" "$srv" "$cli" "SHA256SUMS" "VERSION" )
    echo "    打包: ${pkgbase}.zip"
  else
    ( cd "$outdir" && tar -czf "${pkgbase}.tar.gz" "$srv" "$cli" "SHA256SUMS" "VERSION" )
    echo "    打包: ${pkgbase}.tar.gz"
  fi
  echo "    ✓ $srv / $cli（静态二进制，CGO_ENABLED=0）"
}

# ---------- 主流程 ----------
if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  grep '^#' "$0" | sed 's/^# \{0,1\}//' | head -30
  exit 0
fi

echo "=============================================="
echo " speedTest 构建  v$VERSION"
echo " 配置: $CONFIG_FILE"
echo " 源码: $SRC_DIR"
echo " 输出: $DEST_DIR"
echo "=============================================="

# 读取平台矩阵（忽略注释与空行，格式：平台名 = GOOS/GOARCH [后缀]）
platforms_raw=$(awk -F'=' '/^\[platforms\]/{s=1;next} /^\[/{s=0} s && NF && $0 !~ /^[[:space:]]*;|^[[:space:]]*#/{
  gsub(/[[:space:]]/,"",$1); gsub(/^[[:space:]]+|[[:space:]]+$/,"",$2); print $1"|"$2}' "$CONFIG_FILE")
if [ -z "$platforms_raw" ]; then
  echo "[error] config/version.ini 缺少 [platforms] 平台矩阵" >&2
  exit 1
fi

n=0
while IFS='|' read -r plat spec; do
  [ -z "$plat" ] && continue
  # spec 形如 linux/amd64 [.exe]
  goos="${spec%%/*}"; rest="${spec#*/}"
  goarch="${rest%% *}"; suffix="${rest#* }"
  [ "$suffix" = "$rest" ] && suffix=""
  build_one "$plat" "$goos" "$goarch" "$suffix"
  n=$((n+1))
done <<< "$platforms_raw"

echo ""
echo "=============================================="
echo " 完成：$n 个平台，产物位于 $DEST_DIR"
echo "=============================================="