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

"""This module defines a new JordanSimulator class."""

from pynsim import Simulator
from JWM.components.network import JordanNetwork
import network_setup
import institution_setup
import engine_setup
from data.exog_inputs import ExogenousInputs
from data.parameters import Parameters
from dateutil.relativedelta import relativedelta
import datetime
import time

import logging
log = logging.getLogger(__name__)


class JordanSimulator(Simulator):
    """A Jordan Simulator class (a subclass of the pynsim Simulator class).

    The Jordan Simulator class inherits from the pynsim Simulator class, with additional
    methods to load simulation information, model components, exogenous inputs,
    observations, and engines.

    Args:
        name (string): name of simulation
        scenario (string): scenario id (to match with id for associated exogenous inputs)
        intervention (string): intervention id (to match with id for associated exogenous inputs)
        date_stamp (string): date of model run (as input by user in excel)
        user (string): name of model user

    Attributes:
        name (string): name of simulation.
        scenario_id (string): scenario id (to match with id for associated exogenous inputs).
        intervention_id (string): intervention id (to match with id for associated exogenous inputs).
        date_stamp (string): date of model run (as input by user in excel).
        user (string): name of model user.

    """

    def __init__(self, name, scenario, intervention, date_stamp, user, csf_job, simulation_type, simulation_number,
                 time=True):
        super(JordanSimulator, self).__init__(network=None, record_time=time)
        self.name = name
        self.scenario_id = scenario
        self.intervention_id = intervention
        self.date_stamp = date_stamp
        self.user = user
        self.engine_start_time = {}
        self.job_id = None
        self.run_options = None
        self.simulation_type = simulation_type
        self.simulation_number = simulation_number
        self.csf_job = csf_job

    def set_timestep_information(self, simulation_inputs, s):
        """Set timestep information on simulator object.

        Args:
            simulation_inputs (pandas dataframe): simulation information from models_input.xlsx (sheet: simulation)

        Returns:
            (adds timesteps to existing simulator object)

        """

        # number_of_years = simulation_inputs[simulation_inputs.simulation_name==self.name]['number_of_years'].values[0]
        number_of_years = simulation_inputs.number_of_years[s]
        if number_of_years > 84:
            number_of_years = 84
            logging.info("Number of years set to maximum: 84")
        # start_month = simulation_inputs[simulation_inputs.simulation_name == self.name]['start_month'].values[0]
        start_month = simulation_inputs.start_month[s]

        # simulation_type = simulation_inputs[simulation_inputs.simulation_name==self.name]['simulation_type'].values[0]
        simulation_type = simulation_inputs.simulation_type[s]
        if simulation_type == "tanker":
            start_month = "Jan 2015"
            logging.info("Start year: 2015")
            if number_of_years > 36:
                number_of_years = 36
                logging.info("Number of years set to maximum for tanker water market simulations: 36")

        num_months = 12 * number_of_years -1
        one_month = relativedelta(months=1)

        timesteps = [datetime.datetime.strptime(start_month, '%b %Y')]

        for m in range(num_months):
            new_timestep = timesteps[-1] + one_month
            timesteps.append(new_timestep)

        self.set_timesteps(timesteps)

        print "Simulation length: %s timesteps" % (len(timesteps))

    def set_network(self, network_inputs, human_module_inputs):
        """Set network on simulator object.

        Defines the network for simulator object by identifying shapefiles designated for the network
        in model_setup.xlsx, then processes the shapefiles by calling various functions in
        network_setup.py.

        Args:
            network_inputs (pandas dataframe): network shapefile information from models_input.xlsx (sheet: network)

        Returns:
            (adds network, which is a pynsim network object, to existing simulator)

        """

        network_name = self.name + ' network'
        network = JordanNetwork(name=network_name)

        groundwater_nodes_shpname = network_inputs[network_inputs.simulation_name == self.name]['groundwater_nodes'].values[0]
        network_nodes_shpname = network_inputs[network_inputs.simulation_name == self.name]['network_nodes'].values[0]
        network_links_shpname = network_inputs[network_inputs.simulation_name == self.name]['network_links'].values[0]
        urban_households_shpname = network_inputs[network_inputs.simulation_name == self.name]['urban_households'].values[0]
        highland_farms_shpname = network_inputs[network_inputs.simulation_name == self.name]['highland_farms'].values[0]
        jv_farms_shpname = network_inputs[network_inputs.simulation_name == self.name]['jv_farms'].values[0]
        ww_nodes_shpname = network_inputs[network_inputs.simulation_name == self.name]['ww_nodes'].values[0]

        network_setup.load_groundwater_data()
        network_setup.load_network(groundwater_nodes_shpname, network_nodes_shpname, network_links_shpname,
                                   urban_households_shpname, highland_farms_shpname, jv_farms_shpname, ww_nodes_shpname)

        gw_nodes = network_setup.get_gw_nodes()
        network_nodes = network_setup.get_network_nodes()
        ww_nodes = network_setup.get_ww_nodes()
        urb_nodes = network_setup.get_urb_nodes()
        hfarm_nodes = network_setup.get_hfarm_nodes()
        jvfarm_nodes = network_setup.get_jvfarm_nodes()
        network_links = network_setup.get_network_links()
        gw_network_links = network_setup.get_gw_network_links()
        urb_network_links = network_setup.get_urb_network_links()
        hfarm_gw_links = network_setup.get_hfarm_gw_links()
        jvfarm_network_links = network_setup.get_jvfarm_network_links()

        network_setup.read_res_properties(network_nodes)
        network_setup.read_highland_farm_properties(hfarm_nodes)
        network_setup.read_jv_farm_propertes(jvfarm_nodes)
        network_setup.read_urban_hh_properties(urb_nodes)
        network_setup.load_human_agents(human_module_inputs)

        network.add_nodes(*gw_nodes)
        network.add_nodes(*network_nodes)
        network.add_nodes(*ww_nodes)
        network.add_nodes(*urb_nodes)
        network.add_nodes(*hfarm_nodes)
        network.add_nodes(*jvfarm_nodes)
        network.add_links(*network_links)
        network.add_links(*gw_network_links)
        network.add_links(*urb_network_links)
        network.add_links(*hfarm_gw_links)
        network.add_links(*jvfarm_network_links)

        self.network = network

    def add_institutions_to_network(self):
        """Add institutions to simulator's network.

        Create institutions by calling various functions in institution_setup.py, then adds institutions
        to simulator's network.

        Args:
            None

        Returns:
            (adds institutions to simulator's network)

        """

        all_institutions = institution_setup.get_institution_list()
        all_govs = institution_setup.get_all_gov_institutions(all_institutions)
        all_nodes_of_type_institutions = institution_setup.create_all_nodes_of_type_institutions(self.network)

        self.network.add_institutions(*all_institutions)
        self.network.add_institutions(all_govs)
        self.network.add_institutions(*all_nodes_of_type_institutions)

        institution_setup.add_govs_to_waj(self.network)
        institution_setup.add_gov_wrappers_to_human_wrapper(self.network)

    def add_nodes_links_to_institutions(self):
        """Add nodes and links in simulator's network to associated institutions

        Uses institution designation in nodes/links' shapefiles to assign nodes/links
        to appropriate institution. Designation is provided in nodes/links
        "institution_name" attribute (which is originally defined during network
        setup).

        Args:
            None

        Returns:
            (adds existing nodes/links to existing institutions in simulator)

        """

        # add nodes/links to associated institutions
        for n in self.network.nodes:
            n.add_to_institutions(n.institution_names, self.network)  # add nodes to designated institutions
            if hasattr(n, 'routing_engine'):
                n.add_to_institutions(n.routing_engine, self.network)  # add nodes to appropriate routing institution

        for l in self.network.links:
            l.add_to_institutions(l.institution_names, self.network)  # add links to designated institutions
            if hasattr(l, 'routing_engine'):
                l.add_to_institutions(l.routing_engine, self.network)  # add links to appropriate routing institution


    def add_human_agents_nodes_to_nodes_institutions(self):
        """Add human agents in simulator's network to the human agent wrapper institution, the
        associated governorate institutions, and those institutions to the human agent wrapper
        institution

        So far, the assignment is hard-coded.

        Args:
            None

        Returns:
            (adds existing Amman household agents and nodes to existing institutions and nodes in simulator)

        """

        human_hh_agents = network_setup.get_human_hh_agents()
        human_co_agents = network_setup.get_human_co_agents()
        human_rf_agents = network_setup.get_human_rf_agents()
        human_in_agents = network_setup.get_human_in_agents()
        human_agents_no_industry = list(human_hh_agents)
        human_agents_no_industry.extend(human_co_agents)
        human_agents_no_industry.extend(human_rf_agents)
        human_agents = list(human_agents_no_industry)
        human_agents.extend(human_in_agents)
        self.network.get_institution('human_agent_wrapper').hh_agents = human_hh_agents
        self.network.get_institution('human_agent_wrapper').co_agents = human_co_agents
        self.network.get_institution('human_agent_wrapper').rf_agents = human_rf_agents
        self.network.get_institution('human_agent_wrapper').in_agents = human_in_agents
        self.network.get_institution('human_agent_wrapper').agents = human_agents
        self.network.get_institution('human_agent_wrapper').agents_no_industry = human_agents_no_industry
        gov_wrappers = (i for i in self.network.get_institution("human_agent_wrapper").institutions if
                        i.component_type == 'GovAgentWrapper')
        gov_wrappers = sorted(gov_wrappers, key=lambda i: i.name)
        for gov in range(1, 13):
            gov_wrappers[gov-1].gov_id = gov
            gov_wrappers[gov-1].hh_agents = [x for x in human_hh_agents if x.gov == gov]
            gov_wrappers[gov-1].co_agents = [x for x in human_co_agents if x.gov == gov]
            gov_wrappers[gov-1].rf_agents = [x for x in human_rf_agents if x.gov == gov]
            gov_wrappers[gov-1].in_agents = [x for x in human_in_agents if x.gov == gov]
            gov_wrappers[gov-1].agents = [x for x in human_agents if x.gov == gov]
            gov_wrappers[gov-1].agents_no_industry = [x for x in human_agents_no_industry if x.gov == gov]
            for a in gov_wrappers[gov-1].agents:
                a.governorate = gov_wrappers[gov-1]

        subdist_wrappers = (i for i in self.network.get_institution("human_agent_wrapper").institutions if
                            i.component_type == 'SubdistAgentWrapper')
        subdist_wrappers = sorted(subdist_wrappers, key=lambda i: i.name)
        for subdist_wrapper in subdist_wrappers:
            subdist_code = int(subdist_wrapper.name[8:14])
            subdist_wrapper.subdist_code = subdist_code
            subdist_wrapper.hh_agents = [x for x in human_hh_agents if x.subdist_code == subdist_code]
            subdist_wrapper.co_agents = [x for x in human_co_agents if x.subdist_code == subdist_code]
            subdist_wrapper.rf_agents = [x for x in human_rf_agents if x.subdist_code == subdist_code]
            subdist_wrapper.in_agents = [x for x in human_in_agents if x.subdist_code == subdist_code]
            subdist_wrapper.agents = [x for x in human_agents if x.subdist_code == subdist_code]
            subdist_wrapper.agents_no_industry = [x for x in human_agents_no_industry if x.subdist_code == subdist_code]
            subdist_wrapper.gov = subdist_wrapper.hh_agents[0].gov
            subdist_wrapper.x = subdist_wrapper.hh_agents[0].x
            subdist_wrapper.y = subdist_wrapper.hh_agents[0].y
            for a in subdist_wrapper.agents:
                a.subdist = subdist_wrapper
        self.network.add_components(*human_hh_agents)
        self.network.add_components(*human_co_agents)
        self.network.add_components(*human_rf_agents)
        self.network.add_components(*human_in_agents)

    def set_exogenous_inputs(self):
        """Add exogenous inputs object as an attribute to simulator's network

        Creates an exogenous inputs object using class ExogenousInputs (see
        exog_inputs.py and adds as an attribute to the simulator's network.
        The values of the exogenous inputs class are identified based on scenario
        and intervention id's of the simulator.

        Args:
            None

        Returns:
            (adds exogenous_inputs attribute to simulator's network)

        """
        self.network.simulation_type = self.simulation_type
        self.network.simulation_number = self.simulation_number
        self.network.parameters = Parameters(csf_job=self.csf_job, simulation_type=self.simulation_type,
                                             simulation_number=self.simulation_number)
        self.network.exogenous_inputs = ExogenousInputs(self)
        self.network.exogenous_inputs.load_scenario_data()
        self.network.exogenous_inputs.load_intervention_data()
        self.network.exogenous_inputs.load_human_model_data(simulation_type=self.simulation_type,
                                                            simulation_number=self.simulation_number)
        self.network.exogenous_inputs.load_highland_farm_module_data(self)

    def set_engines(self, engines_inputs):
        """Set engines (and engine sequence) on the simulator.

        Uses engine information and sequencing of engine input in models_input.xlsx
        to set engines on simulator. Actual engines are defined in various files in
        the engines folder.

        Args:
            engines_inputs (pandas dataframe): engine information from models_input.xlsx (sheet: engines)

        Returns:
            (adds engines to simulator)

        """

        no_of_engines = engines_inputs[(engines_inputs.simulation_name == self.name)].shape[0]
        for e in range(no_of_engines):
            engine_class = engines_inputs[(engines_inputs.simulation_name == self.name) &
                                          (engines_inputs.order == e + 1)]['engine_class'].values[0]
            engine_target = engines_inputs[(engines_inputs.simulation_name == self.name) &
                                          (engines_inputs.order == e + 1)]['engine_target'].values[0]

            engine_setup.create_engine(engine_class, engine_target, self)

    def add_timesteps(self, nr_years_to_add):
        from dateutil.relativedelta import relativedelta
        _new_timesteps = [self.current_timestep + relativedelta(months=m + 1) for m in range(nr_years_to_add * 12)]
        self.timesteps += _new_timesteps
        self.network.timesteps = self.timesteps

    def continue_simulation(self):
        _last_timestep_idx = self.current_timestep_idx
        for idx, timestep in enumerate(self.timesteps):
            self.network.set_timestep(timestep, idx + _last_timestep_idx)

            for engine in self.engines:
                logging.debug("Running engine %s", engine.name)

                engine.timestep = timestep
                engine.timestep_idx = idx + _last_timestep_idx
                engine.run()

                self.engine_start_time[time.time] = engine.name

            self.network.post_process()

        logging.debug("Finished")

    def restart(self):
        self.network.reset_history()
        self.reset_component_history()
        self.network.timesteps = self.timesteps
        self.start()

    def reset_component_history(self, **kwargs):
        _component_subset = kwargs.get('components', None)
        if _component_subset:
            _components = _component_subset
        else:
            _components = self.network.components
        for c in _components:
            for k in c._properties:
                c._history[k] = []
