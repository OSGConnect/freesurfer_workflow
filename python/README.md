Various python scripts to handle workflow submission and management

compare_mri.py - script to compare MRI outputs from freesurfer

fsurf-osgconnect  -- script to handle workflow submission and management on 
                     OSG Connect login nodes
fsurf-config -- script to setup configuration for submissions on OSG Connect

create_fsurfer_package.sh -- script to generate rpms of fsurf for OSG Connect

fsurf - REST client to submit and run Freesurfer workflows

freesurfer.py - standalone script to generate a pegasus DAX that does freesurfer image process

fsurf_user_admin.py - script to manage fsurf users (modify, list, create, disable) in PGSQL
                      database for REST tools
                      
purge_inputs.py - script to remove old input files that have been uploaded
                   
purge_results.py - script to remove old results from the filesystem

process_mri.py - script to generate and run  a pegasus workflow for uploaded input files

setup_*.py - python setup scripts
 
update_fsurf_job.py - run at the end of a workflow by pegasus , marks a workflow as complete and does
                      final processing of outputs

warn_purge.py - script that warns user that their results will be removed                   

                      

