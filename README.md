<div align="center">

# Mane

</div>

Sync directories from local and remote machines.

## Tests
| S.No. | Test Case | Expected Result | Result |
| - | - | - | - |
| 1. | `LOG_FILTER` environment variable not defined. | `INFO` level filter enabled. |  |
| 2. | `LOG_FILTER` environment variable defined. | Level filter enabled. |  |
| 3. | Config file passed as an argument and the file doesn't exist. | Create the empty configuration file and start watching. |  |
| 4. | Config file passed as an argument and the file exist. | Start watching the file with debouncer. |  |
| 5. | Config file passed as an argument and the file exist. | Start watching the file with debouncer. |  |
| 6. | Default config file not present. | Create a default config file. |  |
| 7. | Default config file present. | Start watching over the parent directory. |  |
| 8. | Invalid field in config. | Invalid value logged by watcher. |  |
| 9. | No `syncs` defined. | Log that no syncs are defined. |  |
| 10. | Invalid field in `syncs` list. | Invalid value logged by config watcher. |  |
| 11. | `syncs` updated. | Check for added and removed syncs. Start corresponding debouncers + watchers. |  |
| 12. | Target unreachable. | Log target unreachable on debounced event. |  |
