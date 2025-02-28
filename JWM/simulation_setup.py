#    (c) Copyright 2021, Jim Yoon, Christian Klassert, Philip Selby,
#    Thibaut Lachaut, Stephen Knox, Nicolas Avisse, Julien Harou,
#    Amaury Tilmant, Steven Gorelick
#
#    This file is part of the Jordan Water Model (JWM).
#
#    JWM is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    JWM is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with JWM.  If not, see <http://www.gnu.org/licenses/>.

"""Setup simulations based upon input information in model_inputs.xlsx.

This module loads information from model_setup.xlsx as pandas dataframes, and uses the information to
create a list of pynsim simulations. The functions in this module are called in the main model run file,
jordan_prototype.py, and in turn typically call methods in simulator objects (which are defined in
simulator.py).

Functions:
    create_simulations()
    load_network()
    load_institutions()
    load_exogenous_inputs()
    load_observations()
    load_engines()

Attributes:
    model_inputs_xlsx (pandas ExcelFile): Pandas excel file object of models_input.xlsx
    simulation_inputs (pandas dataframe): Pandas dataframe containing simulation information and timesteps
    network_inputs (pandas dataframe): Pandas dataframe containing network info for each simulation
    engines_inputs (pandas dataframe): Pandas dataframe containing engine info for each simulation

"""

from simulator import JordanSimulator
from JWM import get_excel_data
import logging
log = logging.getLogger(__name__)

model_inputs_xlsx = get_excel_data("model_setup.xlsx")
simulation_inputs = model_inputs_xlsx.parse("simulation")
network_inputs = model_inputs_xlsx.parse("network")
engines_inputs = model_inputs_xlsx.parse("engines")
human_module_inputs = get_excel_data("human_module_params.xlsx")

def create_simulations():
    """Create a list of pynsim simulation objects with timestep information.

     Uses information in model_setup.xlsx (sheet: simulation) to create list of simulations,
     including timesteps for each simulation.

    Args:
        none

    Returns:
        simulations (List): pynsim simulator objects
    """

    simulations = []

    for s in range(simulation_inputs.shape[0]):
        if simulation_inputs.run[s] == 'yes':
            simulation_name = simulation_inputs.simulation_name[s]
            scenario_id = simulation_inputs.scenario_id[s]
            intervention_id = simulation_inputs.intervention_id[s]
            date_stamp = simulation_inputs.date_stamp[s]
            user = simulation_inputs.user[s]
            simulation_type = simulation_inputs.simulation_type[s]
            simulation_number = simulation_inputs.simulation_number[s]
            simulation = JordanSimulator(name=simulation_name, scenario=scenario_id, intervention=intervention_id,
                                         date_stamp=date_stamp, user=user, csf_job=None,
                                         simulation_type=simulation_type, simulation_number=simulation_number,
                                         time=True)
            simulation.set_timestep_information(simulation_inputs, s)

            try:
                _job_id = simulation_inputs.csf_job[s]
                # _run_options = simulation_inputs.run_options[s]
                simulation.job_id = _job_id
                # simulation.run_options = _run_options
            except:
                log.warning('job_id and run_options not found in model_setup.xlsx')

            simulations.append(simulation)

    return simulations


def create_simulations_csf(csf_job_nr):
    """Create a list of pynsim simulation objects with timestep information.

     Uses information in model_setup.xlsx (sheet: simulation) to create list of simulations,
     including timesteps for each simulation.

     **** N.B. Runs simulation matching csf_job_nr, does not use the 'run' column. ****

    Args:
        none

    Returns:
        simulations (List): pynsim simulator objects
    """

    simulations = []

    for s in range(simulation_inputs.shape[0]):
        if simulation_inputs.run[s] == 'yes':  # PDS: THIS LINE TO MATCH CSF JOB NUMBER
            simulation_name = simulation_inputs.simulation_name[s]
            scenario_id = simulation_inputs.scenario_id[s]
            intervention_id = simulation_inputs.intervention_id[s]
            date_stamp = simulation_inputs.date_stamp[s]
            user = simulation_inputs.user[s]
            simulation = JordanSimulator(name=simulation_name, scenario=scenario_id, intervention=intervention_id,
                                         date_stamp=date_stamp, user=user, csf_job=csf_job_nr, time=True)
            simulation.set_timestep_information(simulation_inputs, s)

            simulations.append(simulation)

    return simulations

def load_network(simulations=[]):
    """Load the network on each simulation based on network information in model_setup.xlsx (sheet: network)

    Args:
        simulations (List): pynsim simulator objects (default [])

    Returns:
        (modifies existing simulator objects in simulation list)
    """
    for s in simulations:
        logging.info('Network inputs:')
        logging.info(network_inputs)

        s.set_network(network_inputs, human_module_inputs)

        s.network.timesteps = s.timesteps

def load_institutions(simulations):
    """Load the institutions on each simulation

    Args:
        simulations (List): pynsim simulator objects (default [])

    Returns:
        (modifies existing simulator objects in simulation list)
    """
    for s in simulations:
        s.add_institutions_to_network()
        s.add_nodes_links_to_institutions()
        s.add_human_agents_nodes_to_nodes_institutions()  # CK160302

def load_exogenous_inputs(simulations):
    """Load exogenous inputs on each simulation as defined in exog_inputs.xlsx

    Args:
        simulations (List): pynsim simulator objects (default [])

    Returns:
        (modifies existing simulator objects in simulation list)
    """
    for s in simulations:
        s.set_exogenous_inputs()

def load_engines(simulations):
    """Load engines for each simulation

    Args:
        simulations (List): pynsim simulator objects (default [])

    Returns:
        (modifies existing simulator objects in simulation list)
    """
    for s in simulations:
        s.set_engines(engines_inputs)


