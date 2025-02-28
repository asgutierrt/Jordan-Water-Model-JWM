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

from pynsim import Node
import logging
import numpy as np
from JWM import get_excel_data

class JordanNode(Node):
    """The Jordan node class.

    The Jordan node class is a parent class that provides common methods for nodes in the Jordan model.

    **Properties**:

        |  *institution_names* (list) - list of institutions associated with node

    """
    description = "Common Methods for Jordan Nodes"

    def __init__(self, name, **kwargs):
        super(JordanNode, self).__init__(name, **kwargs)
        self.institution_names = []

    def add_to_institutions(self, institution_list, n):
        """Add node to institutions.

        Adds node to each institution in a list of Jordan Institution objects.

        **Arguments**:

        |  *institution_list* (list) - list of institutions associated with node
        |  *n* (pynsim network) - network

        """
        for institution in n.institutions:
            for inst_name in institution_list:
                if institution.name.lower() == inst_name.lower():
                    institution.add_node(self)



class Groundwater(JordanNode):
    """The Groundwater node class.

    A groundwater node is representative of a unit stress/observation location in the groundwater system. For the Jordan
    model, there is one groundwater node per subdistrict, per usage type (agricultural or urban), per layer. The groundwater
    head response at each node is calculated via the GWResponseEngine, which consolidates pumping values at all groundwater
    nodes in the system and calculates system response based upon a response matrix approach. Each node contains a groundwater
    lift value, which also account for in-well drawdown and a bias correction to match starting head conditions in 2009.

    **Properties**:

        |  *pumping* (int) - pumping volume for current time step [m3/s]
        |  *lift* (int) - lift, i.e. distance from pumping water level to ground surface [m]
        |  *head* (int) - lift, i.e. distance from pumping water level to ground surface [m]
        |  *check_dry* (bool) - boolean to check whether aquifer dry (pumping water level < aquifer bottom)

    """

    def __init__(self, name, **kwargs):
        super(Groundwater, self).__init__(name, **kwargs)
        self.name = name  # Set the name as a unique groundwater node identifier (subdistrict_layer_type)
        self.head = 0
        self.head_bias_corr = 0
        self.pumping = 0
        self.lift = 0
        self.check_dry = None
        self.permanently_dry = 0
        self.CapReductFile = None

    _properties = {
        'pumping': 0,
        'head': 0,
        'head_bias_corr': 0,
        'lift': 0,
        'check_dry': '',
        'capacity_reduction_factor': 0,
        }
                
    def setup(self, timestep):
        """Setup for groundwater node.

        Sets the initial head value (along with lift) for the time step, resets pumping to zero, checks whether node
        is dry using last time step head value.

        """
        self.head = self._gw_node_properties['head_calc'][self.network.current_timestep_idx]
        self.head_bias_corr = self.head + self._gw_node_properties['bias_correction']
        self.pumping = 0
        self.lift = self._gw_node_properties['top'] - self.head_bias_corr

        # Check if dry
        if self.head_bias_corr < self._gw_node_properties['bot'] + (.20 * (self._gw_node_properties['top'] -
                                                                           self._gw_node_properties['bot'])):
            self.check_dry = True
            self.permanently_dry = 1
        else:
            self.check_dry = False

        # Calculate capacity reduction factor
        if self.head_bias_corr < self._gw_node_properties['bot'] + \
                (self.network.parameters.gw['gw_reduction_start'] *
                 (self._gw_node_properties['top']-self._gw_node_properties['bot'])):
            self.capacity_reduction_factor = \
                (self.head_bias_corr - (self._gw_node_properties['bot'] +
                                        (.20 * (self._gw_node_properties['top'] - self._gw_node_properties['bot'])))) /\
                    (self._gw_node_properties['bot'] +
                     (self.network.parameters.gw['gw_reduction_start'] *
                      (self._gw_node_properties['top'] - self._gw_node_properties['bot'])) - \
                        (self._gw_node_properties['bot'] +
                         (.20 * (self._gw_node_properties['top'] - self._gw_node_properties['bot']))))
            if self.capacity_reduction_factor <= 0.:
                self.capacity_reduction_factor = 0
                self.permanently_dry = 1

        else:
            self.capacity_reduction_factor = 1

        if self.permanently_dry > 0:
            self.check_dry = True
            self.capacity_reduction_factor = 0

        # Set lifts to zero for abnormal groundwater locations with excessively high head
        abnormal_list = ['120201_urb_01', '120201_ag_01', '120301_ag_01', '120301_urb_01', '120101_urb_01',
                         '340201_urb_12', '240201_urb_01', '240101_urb_01', '120201_ag_07']
        if self.name in abnormal_list:
            if self.head_bias_corr > self._gw_node_properties['top']:
                self.head_bias_corr = self._gw_node_properties['top']
        if self.name in abnormal_list:
            if self.lift < 0:
                self.lift = 0


class Reservoir(JordanNode):
    """The Reservoir node class.

    A reservoir node is representative of a surface water reservoir.

    **Properties**:

        |  *live_storage_capacity* (int) - live storage capacity [m3]
        |  *inflow* (int) - inflow into reservoir (pre-processed from surface water model) [m3]
        |  *release* (int) - release from reservoir [m3]
        |  *volume* (int) - stored water volume in reservoir [m3]
        |  *level* (int) - water level in reservoir [m3]
        |  *surface_area* (int) - live storage capacity [m3]

    """

    description = "Reservoir"

    def __init__(self, name, **kwargs):
        super(Reservoir, self).__init__(name, **kwargs)
        self.name = name
        self.live_storage_capacity = None  # live storage capacity [m3]
        self.max_elevation = None  # max water elevation [m]
        self.height = None  # reservoir height [m]
        self.year = None  # year of construction
        self.sediment = None  # sediment accumulation rate
        self.timestep = 0

    res_property_indices = ['live_storage_capacity', 'max_elevation', 'height', 'year', 'sediment',
                            'min_storage', 'avg_inflow', 'avg_evap', 'avg_seep', 'max_release',
                            'vol_area_c1', 'vol_area_c2', 'vol_area_c3', 'temp_pan_evap_c1',
                            'temp_pan_evap_c2']

    _inflow = {}

    _properties = {
        'live_storage_capacity': 0,  # live storage capacity [cubic meters]
        'inflow': 0,  # inflows pre-processed from surface water model
        'release': 0,
        'volume': 0,
        'level': 0,
        'surf_area': 0,
        'evap': 0,
        }

    def setup(self, timestep):
        """Setup for reservoir node.

        Sets the initial reservoir volume data if timestep = 0.

        """
        if self.network.current_timestep_idx == 0:
            if self.name <> 'tiberias_res':  # Set reservoirs equal to average volumes for strat of the model run
                self.volume = self.network.observations.volume_averages[self.name][timestep.month]

        # evaporation calculation

        # get temperature values (use average if no temp data available)
        if self.name == 'talal_res' or self.name == 'mujib_res' or self.name == 'walah_res' or self.name == 'karameh_res':
            self.t = self.network.observations.temp_averages['AM0007'][self.network.current_timestep.month]
        elif self.name == 'wehdah_res':
            self.t = self.network.observations.temp_averages['Wehdah (AD0034)'][self.network.current_timestep.month]
        else:
            self.t = self.network.observations.temp_averages['AL0035'][self.network.current_timestep.month]

        # calculate surface area from volume
        self.surf_area = (self.res_properties['vol_area_c1'] * (self.volume**3)) + \
                         (self.res_properties['vol_area_c2'] * (self.volume**2)) + \
                         (self.res_properties['vol_area_c3'] * (self.volume))

        # calculate daily pan evaporation (in mm) from temp
        pan_evap = (self.t * self.res_properties['temp_pan_evap_c1']) + self.res_properties['temp_pan_evap_c2'] \
            if ((type(self.res_properties['temp_pan_evap_c1']) != str) and
                (type(self.res_properties['temp_pan_evap_c2']) != str)) else 0

        # calculate evaporation from surface area and pan evap
        lake_to_pan_coeff = 1.3
        if self.name != "tiberias_res":
            self.evap = self.surf_area * pan_evap * 30.4375 / (lake_to_pan_coeff * 1000)
        else:
            self.evap = 0

        self.evap *= self.network.parameters.sw['evap_factor']

        # evap can't be less than zero
        if self.evap < 0:
            self.evap = 0


    def get_releases(self):
        for i in self.institutions:
            release = i.release[self.name]


    def run_mujib_walah_balance(self):  # calculates new reservoir volume based on inflows, releases, and losses (ET, leakage, and spills))
        """Calculate new reservoir volume.

        Calculates new reservoir volumes based on inflows and outflows for the time period.

        """

        # calculate reservoir balance (for reservoirs in the main JVA system, balance calculated in JVA optimization)

        year = self.network.current_timestep.year
        month = self.network.current_timestep.month

        # For Mujib and Walah, calculate released based on WAJ results
        releases = {}
        releases['mujib_res'] = self.network.get_institution('waj').real_extraction['zara_maeen'] * self.network.get_institution('jva').mujib_annual_availability / \
                                self.network.get_institution('waj').yearly_max_groundwater_pumping['zara_maeen']
        releases['walah_res'] = self.network.get_institution('waj').real_extraction['zara_maeen'] * self.network.get_institution('jva').walah_annual_availability / \
                                self.network.get_institution('waj').yearly_max_groundwater_pumping['zara_maeen']

        # If release causes either reservoir to go into the negative, assign release to the other reservoir (which hopefully has enough storage volume!)

        if releases[self.name] > self.volume - self.evap - self.res_properties['avg_seep'] + self.inflow:
            diff = releases[self.name] - (self.volume - self.evap - self.res_properties['avg_seep'] + self.inflow)
            releases[self.name] = self.volume - self.evap - self.res_properties['avg_seep'] + self.inflow
            if self.name == 'walah_res':
                releases['mujib_res'] += diff
            else:
                releases['walah_res'] += diff

        self.volume = self.volume - self.evap - self.res_properties['avg_seep'] - releases[self.name] + self.inflow


class Junction(JordanNode):
    pass


class AgDemand(JordanNode):
    pass


class UrbDemand(JordanNode):
    """The Urban Demand node class.

    An urban demand node aggregates urban demands at a node in the main network.

    **Properties**:

        |  *demand_request* (int) - aggregated demand request [m3]
        |  *demand* (int) - aggregated demand [m3]

    """

    description = "A node that aggregates demands from nearest disconnected urban agent nodes"

    _properties = {
                   'demand_request': 0,
                   'demand': 0,
                   }
class WTP(JordanNode):
    pass

class WWTP(JordanNode):
    """The WWTP node class.

    A WWTP node is representative of a wastewater treatment plant.

    **Properties**:

        |  *influent* (int) - monthly influent into wastewater treatment plant [m3]
        |  *effluent* (int) - monthly effluent from wastewater treatment plant [m3]
    """
    def __init__(self, name, **kwargs):
        super(WWTP, self).__init__(name, **kwargs)
        self.name = name  # Set the name as a unique groundwater node identifier (subdistrict_layer_type)
        self.effluent = {}
        self.effluent_history = []
        self.influent = 0
        self.influent_history = []

    _properties = {
        'effluent': {},
        'influent': {},
    }

    def setup(self, timestep):
        """Setup for reservoir node.

        Sets the initial reservoir volume data if timestep = 0.

        """
        pass

