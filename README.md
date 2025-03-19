# cat-demo-param-dispatch

A basic demo of dispatching multiple processing jobs in a pipeline.


## Concept

We have a standalone processing capsule (not this one) with a data asset attached containing many files/sessions for analysis. The capsule processes one file or session in the asset, and has an app panel for modifying analysis parameters. 

We want to run this capsule for many combinations of parameters and do something like:
```python
n_units_list = [0, 5, 10]
areas = ['VISp', 'AUDp']
session_ids = ['a', 'b', 'c']

for n_units in n_units_list:
    for area in areas:
        for session_id in session_ids:
            run_capsule(capsule_id, session_id, area, n_units)
```

This could be implemented via the Code Ocean API, but the results of each capsule run would not be grouped together, and it would be difficult to monitor overall progress and errors.

--- 

To run the equivalent in a pipeline, we write sets of parameters as .json files (in this capsule):
```python

for n_units in n_units_list:
    for area in areas:
        for session_id in session_ids:
            write_parameters_file(session_id, area, n_units)
```
each parameters file is then passed to a parallel instance of the processing capsule, which just does:
```python
process(parameters_file)
```