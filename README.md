<div align="center">

# Mane

</div>

Sync directories from local and remote machines.

## Tests
| S.No. | Test Case | Expected Result | Result |
| - | - | - | - |
| 1. | `LOG_FILTER` environment variable not defined. | `INFO` level filter enabled. |  |
| 2. | `LOG_FILTER` environment variable defined. | Level filter enabled. |  |
| 3. | Config file passed as an argument and the file doesn't exist. | Log that the file doesn't exist. |  |
| 4. | Config file passed as an argument and the file exist. | Compare hash and proceed. |  |
| 5. | Default config file not present. | Log that the file doesn't exist. |  |
| 6. | Default config file present. | Compare hash and proceed. |  |
| 7. | Invalid field in config. | Log error. |  |
| 8. | No `syncs` defined. | Log that no syncs are defined. |  |
| 9. | Invalid field in `syncs` list. | Invalid value logged by config watcher. |  |
| 10. | `syncs` updated. | Check for added and removed syncs. Start corresponding debouncers + watchers. |  |
| 11. | Target unreachable. | Log target unreachable. |  |
