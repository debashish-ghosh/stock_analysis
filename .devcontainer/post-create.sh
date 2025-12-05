#! /usr/bin/bash

# Setup uv project
uv sync --dev

# Enable git completions
GIT_VERSION=$(git --version | awk '{print $3}')
GIT_COMPLETIONS_URL="https://raw.githubusercontent.com/git/git/v${GIT_VERSION}/contrib/completion/git-completion.bash"
curl -o ~/.git-completion.bash $GIT_COMPLETIONS_URL
echo "source ~/.git-completion.bash" >> ~/.bashrc