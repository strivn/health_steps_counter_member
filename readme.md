# Health Steps Counter - Member

--- 

The member script reads exported Apple Health Data, seek for step count data, and create a differentially private dataset to share with the aggregator datasites. 

Built on top of Syftbox, see: https://syftbox-documentation.openmined.org

## Steps to Install 
1. Run `syftbox app install strivn/health_steps_counter_member`
2. Export data from Apple Health (go to Apple Health, profile, export data)
3. Save it on your local device
4. Copy `config.yaml.template` and save it as `config.yaml`
5. Change 'filepath' to the export data location
6. Feel free to change epsilon, but the rest aren't "changeable" just yet (to be updated on further iterations!)


## Config File 
- `api_name`: `'health_steps_counter'` / do not change, placeholder for further use cases
- `aggregator_datasite`: / change aggregator datasite
- `filepath`: `'[PATH_TO_APPLE_HEALTH_EXPORT]'` currently only step count is supported, placeholder for further use cases
- `parameters`:
  - `type`: `'HKQuantityTypeIdentifierStepCount'` / currently only step count is supported, placeholder for further use cases
  - `epsilon`: `0.5` / change to any non-negative number 
  - `bounds`: `'auto-local'` / currently only 'auto-local' is supported, placeholder for further use cases


