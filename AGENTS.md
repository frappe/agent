# Agent - Agent Notes

## Pull requests

Always open a pull request against `develop`. Add the `backport-master` label.

```bash
gh pr create --base develop --label backport-master
```

Do not open a pull request against `master`. When a pull request on `develop`
merges, Mergify opens the backport to `master` (see `.mergify.yml`). A pull
request on `master` skips `develop`, and someone must backport it by hand.

Do not add `backport-master` to a pull request that targets `master`. Mergify
then opens a duplicate backport to `master`.
