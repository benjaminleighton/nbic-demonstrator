import os
import time

# Provena config
stage = "DEV"
kc_endpoint = "https://auth.dev.provena.nbic.cloud/auth/realms/provena"
registry_endpoint = "https://registry-api.dev.provena.nbic.cloud"
provenance_endpoint = "https://prov-api.dev.provena.nbic.cloud"
data_store_endpoint = "https://data-api.dev.provena.nbic.cloud"

#def create_or_fetch_organisation(orgname):
#def create_or_fetch_person(username):

class xr():
    def open_zarr(path):
        return None
    def to_zarr(path):
        return None

class Provenance():

    # This is just for prototyping see below for how a proposal on how these would be fetched or created on demand
    path_id_records = {
        "temperature" : ("10378.1/1740705", "10378.1/1741057"),
        "humidity" : ("10378.1/1740705", "10378.1/1741060"),
        "wind_speed" : ("10378.1/1740705", "10378.1/1741058"),
        "mc_adf_path" : ("10378.1/1740705", "10378.1/1741059"),
        "ffdi" : ("10378.1/1740705", "10378.1/1741061"),        
    }
    
    def __init__(self):
        self.inputs = {}
        self.outputs = {}
        self.start_time = int(time.time())
    
    def _create_or_fetch_person_org(self):
        """
        if the person or organisation doesn't exist create them
        """
        #user = os.environ['JUPYTERHUB_USER']
        #org = 'CSIRO' <- _create_or_fetch
        person = "10378.1/1740702" #create_or_fetch_person(
        organisation = "10378.1/1727883" #create_or_fetch_organisation(
        return person, organisation

    def _create_or_fetch_dataset_record(self, candidate_path, name_in_workflow):
        """
        The candidate path is an external path of a managed or to be managed dataset
        e.g "s3://nbic1-stage-shared-artifacts/nbic-stage1/weather/projected/AU_hourly_temperature_C.zarr"
        The logic here looks up candidate records matching that path and if there is one chooses it
        otherwise prompts the user if there is more than one. If there isn't one it registers the dataset
        retrospectively as required.
        
        Also derive, find, register a corresponding dataset template. Allthough I'm not sure what they are or why they are necessary?
        """
        ds_id, ds_temp_id = self.path_id_records[name_in_workflow] # this is just for prototyping we would create or fetch based on candidate_path
        fetched_ds = registry.fetch_dataset(registry_endpoint=registry_endpoint, id=ds_id, auth=get_auth()) # we should create or fetch
        fetched_ds_template = registry.fetch_dataset_template(registry_endpoint=registry_endpoint, id=ds_temp_id, auth=get_auth())  # we should create or fetch
        return ds_id, ds_temp_id, fetched_ds, fetched_ds_template, fetched_ds["collection_format"]["dataset_info"]["access_info"]["uri"]

    def register_input_dataset(self, candidate_path, name_in_workflow):
        ds_id, ds_temp_id, fetched_ds, fetched_ds_template, fetched_path = self._create_or_fetch_dataset_record(candidate_path, name_in_workflow)
        self.inputs[name_in_workflow] = (ds_id, ds_temp_id, fetched_ds, fetched_ds_template, fetched_path)
        return fetched_path                                                          
    
    def register_output_dataset(self, candidate_path, name_in_workflow):
        ds_id, ds_temp_id, fetched_ds, fetched_ds_template, fetched_path = self._create_or_fetch_dataset_record(candidate_path, name_in_workflow) 
        self.outputs[name_in_workflow] = (ds_id, ds_temp_id, fetched_ds, fetched_ds_template, fetched_path)
        return fetched_path        
    
    def dataset_dict_scaffold(self, ds_id, ds_template_id, path):
        return {
            "dataset_template_id": ds_template_id ,
            "dataset_id": ds_id,
            "dataset_type": "DATA_STORE",
            "resources": {
                "path": path
            }
        }
    
    def model_run_payload(self, description, start_time=None, end_time=None):
        """
        We can build the model run payload from basic input and output information matching our fetched or created template and datasets
        and dataset templates
        """
        if start_time is None:
            start_time = self.start_time
        if end_time is None:
            end_time = int(time.time())
        person, organisation = self._create_or_fetch_person_org()
        workflow_dict = {
          "workflow_template_id": self._create_or_fetch_workflow_template_record()[1],
          "inputs": [self.dataset_dict_scaffold(dataset_info[0], dataset_info[1], dataset_info[4]) for dataset_info in self.inputs.values()],
          "outputs": [self.dataset_dict_scaffold(dataset_info[0], dataset_info[1], dataset_info[4]) for dataset_info in self.outputs.values()],
          "associations": { "modeller_id" : person, "requesting_organisation_id": organisation },
          "description" : description, # preferrably automatically links to this code in git
          "start_time": start_time,
          "end_time": end_time
        }
        return workflow_dict
    
    def _create_or_fetch_workflow_template_record(self):
        """"
        Given we know the inputs and the outputs we can create a workflow template record or fetch one
        """
        # Now lets search for this template an see if it exists
        # There could be multiple options here to
        # automatically create a new template
        # show near matched templates 
        # TODO Implement similarity search
        # for now just get a pre-created workflow_template that matches the one above^
        ds_temp_id = config.workflow_configuration.workflow_template # we should create or fetch this is just for prototyping
        # lets build a workflow template from the inputs and outputs
        ds_temp = registry.fetch_model_run_workflow_template(registry_endpoint=registry_endpoint, id=ds_temp_id, auth=get_auth())
        return ds_temp, ds_temp_id

        
        
        