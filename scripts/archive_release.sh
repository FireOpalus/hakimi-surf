#!/usr/bin/env bash
set -euo pipefail

# git archive stops at gitlinks. Export the commit each parent recorded, even
# when a developer has checked out a different (or dirty) module working tree.
export_tree() {
    local repo=$1 revision=$2 destination=$3 root_link=${4:-} entry header path pin child
    mkdir -p "$destination"
    git -C "$repo" archive "$revision" | tar -x -C "$destination"
    while IFS= read -r -d '' entry; do
        header=${entry%%$'\t'*}
        [[ $header == '160000 commit '* ]] || continue
        pin=${header##* }
        path=${entry#*$'\t'}
        child="$repo/$path"
        if [[ ! -e "$child/.git" ]] || ! git -C "$child" cat-file -e "$pin^{commit}" 2>/dev/null; then
            # Initialize from the release checkout, including previously missing grandchildren.
            git submodule update --init --recursive -- "${root_link:-$path}" >&2
        fi
        export_tree "$child" "$pin" "$destination/$path" "${root_link:-$path}"
    done < <(git -C "$repo" ls-tree -rz "$revision")
}

revision=${1:?expected a Git revision}
context=${2:-.}
if [[ $context != . ]]; then
    exec git archive --format=tar "$revision:$context"
fi
staging=$(mktemp -d)
trap 'rm -r -- "$staging"' EXIT
export_tree . "$revision" "$staging"
if [[ -f "$staging/scripts/platform_checkout.py" ]]; then
    PYTHONDONTWRITEBYTECODE=1 python3 "$staging/scripts/platform_checkout.py" check
fi
tar -c -C "$staging" .
