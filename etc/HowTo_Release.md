# HowTo: Ship a Release

This assumes there is already a tagged commit for the release.
If there isn't one, do that first, then come back here.

# 1. Create the Release on Git_Hub

# 2. Build Artifacts

1. Check out blobcache in a separate repository or worktree
2. `git checkout < tag e.g. v0.0.x >`
3. `just release-build`

# 3. Publish Artifacts

1. Make sure you are logged in to the container registry
`podman login`
- User is your GitHub user name
- Password is your GitHub personal access token.

2. `just release-publish`
