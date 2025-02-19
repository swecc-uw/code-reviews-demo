#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 <number_of_commits>"
    exit 1
fi

n=$1

current_branch=$(git symbolic-ref --short HEAD)
remote_url=$(git remote get-url origin)

if [[ $remote_url == *"github.com"* ]]; then
    # Convert SSH URL to HTTPS if necessary
    if [[ $remote_url == git@* ]]; then
        repo_path=$(echo $remote_url | sed 's/git@github.com://')
        repo_path=${repo_path%.git}
        remote_url="https://github.com/$repo_path"
    fi
else
    echo "Error: Remote URL is not from GitHub"
    exit 1
fi

base_sha=$(git rev-parse HEAD~$n)

git branch -D demo-base 2>/dev/null || true
git checkout -b demo-base $base_sha

git checkout $current_branch
git branch -D demo-feature 2>/dev/null || true
git checkout -b demo-feature

git push -f origin demo-base
git push -f origin demo-feature

if command -v gh &>/dev/null; then
    echo "Creating PR using GitHub CLI..."
    gh pr create \
        --base demo-base \
        --head demo-feature \
        --title "Demo PR: Last $n commits" \
        --body "This is a demo PR containing the last $n commits from main branch."
else
    pr_url="${remote_url}/compare/demo-base...demo-feature?quick_pull=1"
    echo "GitHub CLI not found. Open this URL to create the PR:"
    echo $pr_url
fi

git checkout $current_branch

echo "Done! Branches created and PR initiated."

