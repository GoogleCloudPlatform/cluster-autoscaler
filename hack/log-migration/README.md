#  Context Migration Tool

This tool was created to facilitate logging migration in CA. It can be used to propagate Go's `context` to functions deep down the call stack. It uses AST analysis to do that, so the changes are guaranteed to work and be minimal. It also has a feature to migrate logs to structural logging.

## Overview

The tool takes two arguments: `-entrypoint` and `-path`. It identifies all functions in `path` subdirectory that currently log something, and that are called (maybe indirectly) by `entrypoint`. It then propagates context from `entrypoint` to those functions, in the following way:

```go
    // function declarations
    // before
    func (...) someFunctionBetweenEntrypointAndTarget(args...) {
    // after
    func (...) someFunctionBetweenEntrypointAndTarget(ctx context.Context, args...) {
```

```go
    // function usages
    //before
    someFunctionBetweenEntrypointAndTarget(args...)
    //after
    someFunctionBetweenEntrypointAndTarget(ctx, args...)
```

If `-migrate-logs` is provided, it converts logging previously done by `klog` (`klog.Infof`, `klog.Warningf`, `klog.Errorf`) inside modified methods into calls to contextual logger (`logger.Info`, `logger.Error`) forms. It also tries its best to migrate the log message. Note: log message rewriting is very rudimentary as we cannot possibly predict every possible log format used in the codebase. Thus, the script may produce unnatural-sounding log messages. You should carefully review the changes produced when this flag is enabled. 

## Usage

To migrate all functions in folder `cloudprovider/gce` that are called from methods `RunOnce` and `Cleanup` and also migrate logs, run:

```bash
go run . -path="./cloudprovider/gce/..." -entrypoint="StaticAutoscaler.RunOnce,GceCloudProvider.Cleanup" -migrate-logs
```

### Flags

- `-entrypoint` (default: `"StaticAutoscaler.RunOnce"`): Comma-separated list of function and method entrypoints defining the originating node in the AST traversal execution.
- `-path` (default: `"./..."`): Standard filepath or glob pattern indicating target scopes to be mutated.
- `-migrate-logs` (default: `false`): Specifically toggles the active rewriting of static `klog` nodes into localized contextual arrays.

## Testing

To validate the script locally, simply run:

```bash
./run_e2e.sh
```

This copies example script in `before/` directory, runs the script on it, saves the result in `actual/`, and then compares the result with the script contained in `expected/`.

The test script also checks if resulting program builds.