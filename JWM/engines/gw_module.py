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

from pynsim import Engine
import numpy as np
from JWM import basepath, get_numpy_data, get_excel_data
import logging
import os

class GWResponseEngine(Engine):
    """An engine class to run the groundwater response matrix calculation on all groundwater nodes in the system.

    The GWResponseEngine class is a pynsim engine that determines the response of the groundwater system (i.e. all groundwater
    nodes in the system) to groundwater pumping. The engine loads head and drawdown response information from external
    numpy arrays, consolidates pumping at each groundwater node in the system, then runs a matrix calculation to determine
    new heads at each groundwater node in the system in response to groundwater pumping.

    Args:
        None

    Attributes:
        baseline_heads (numpy array, 249 x 600): time series of baseline heads (600) for each unit response loc (249)
        dd (numpy array, 249 x 249 x 600): time serioes of baseline heads (600) at each unit response loc (249) for one unit of pumping at each unit response loc (249)
        head (numpy array, 249 x 600): time series of updated heads (600) for each unit response loc (249) (updates at each time step)
        head_proj (numpy array, 249 x 600): time series of projected heads (600) for each unit response loc (249)
        pumping (numpy array, 249 x 1): pumping (converted to unit pumping values) for current time step for each unit response location (249)
        pumping_proj (numpy array, 249 x 600): projection of pumping (converted to unit pumping values for each unit response location (249)

    """

    baseline_heads = get_numpy_data('baseline_heads_extended.npy')  # 249 x 600 numpy array
    dd_pt1 = get_numpy_data('ddmatrix_extended_pt1.npy')
    dd_pt2 = get_numpy_data('ddmatrix_extended_pt2.npy')
    dd = np.concatenate((dd_pt1, dd_pt2))  # 249 x 249 x 600 numpy array
    head = baseline_heads  # 249 x 600 numpy array (tracks head values based upon actual and future baseline)
    head_proj = baseline_heads  # 249 x 600 numpy array (tracks head values based upon actual and future projections as provided by agents)
    pumping = np.zeros((249, 1))  # 249 numpy array to be loaded with unit pumping values for timestep
    pumping_proj = np.zeros((249, 1021))  # 249 x 600 numpy array to be loaded with unit pumping values for timestep
    
    def run(self):
        """ Run the Groundwater Response Engine. The target of this engine are all groundwater nodes in the system.
        """

        timestep = self.target.network.current_timestep_idx + 1  # variable indicating timestep number rather than date (assume timestep starts with value of 1 for first timestep)

        logging.info("Starting GWResponseEngine: timestep " + str(timestep))

        # Step 1) get old heads from gw nodes, consolidate pumping values from agents
        for i in self.target.nodes:  # assumes i've initialized engine with all gw nodes institution

            row = i._gw_node_properties['matrix_position']  # get position of gwnode in array
            self.head[int(row)] = i._gw_node_properties['head_calc']
            self.pumping[int(row)] = ((i.pumping) - i._gw_node_properties['baseline_pumping']) / 0.01

            # For mukheibeh wells, adjust pumping to baseline
            if i.name == '210401_ag_02':
                self.pumping[int(row)] = 0

            # For azraq urb wells, shift 25% of pumping from layer 1 to layer 2
            if i.name == '130104_urb_01':
                self.pumping[int(row)] = ((i.pumping *.75) - i._gw_node_properties['baseline_pumping']) / 0.01

            if i.name == '130104_urb_02':
                self.pumping[int(row)] = ((i.pumping + (self.target.network.get_node('130104_urb_01').pumping * .25)) - i._gw_node_properties['baseline_pumping']) / 0.01

            # For maan, adjust to baseline to account for governmental irrigation pumping

            if i.name == '330103_ag_01':
                self.pumping[int(row)] = 0
         
        # Step 2) run response matrix calculation to determine drawdowns from pumping in current timestep
        addhead = np.zeros((249, 1021))
        if timestep == 1:
            addhead[:, :] = addhead[:, :] + -1.0 * np.einsum('ijk,il ->ljk', self.dd, self.pumping * self.target.network.parameters.gw['gw_response_factor'])
        else:
            addhead[:, 0:timestep-1] = 0
            addhead[:, timestep-1:1021] = addhead[:, timestep-1:1021] + -1.0 * np.einsum('ijk,il ->ljk', self.dd[:, :, 0:1021-timestep+1], self.pumping * self.target.network.parameters.gw['gw_response_factor'])
        # Step 3) add drawdowns to old heads to calculate new heads
        self.head = self.head + addhead
        # Step 4) update head calculation property in gw nodes
        for i in self.target.nodes:
            row = i._gw_node_properties['matrix_position']  # get position of gwnode in array
            i._gw_node_properties['head_calc'] = self.head[int(row)]
            

class DrainResponseEngine(Engine):
    """An engine class to run a drain flow (spring flows + baseflow) for all drain locations in the model.

    The DrainResponseEngine class is a pynsim engine that determines the response of drains in the groundwater model
    based upon calculated heads from the groundwater module.

    Args:
        TBD
    Attributes:
        TBD

    """
    drain_xlsx = get_excel_data("gw_drains.xlsx")
    drains = drain_xlsx.parse('drains_gis')

    def run(self):
        """ Run the Drain Response Engine. The target of this engine are all groundwater nodes in the system.
        """
        self.target.drain_flow = {}
        for basin in self.drains['BASIN_NAME'].unique():
            self.target.drain_flow[basin] = 0
            if len(self.drains[(self.drains.BASIN_NAME==basin)].SUBDIST_CO.unique()) == 1 and self.drains[(self.drains.BASIN_NAME==basin)].SUBDIST_CO.unique()[0] == 0:
                diff = 0
            else:
                for subdist in self.drains[(self.drains.BASIN_NAME==basin)].SUBDIST_CO.unique():
                    count = 0
                    diff = 0
                    for k in self.drains[(self.drains.BASIN_NAME==basin) & (self.drains.SUBDIST_CO ==subdist)].K.unique():
                        for g in self.target.get_institution('all_gw_nodes').nodes:
                            if float(g.name[0:6]) == subdist and float(g.name[-2:]) == k:
                                if self.target.current_timestep_idx == 0:
                                    diff += 0
                                    count += 1
                                else:
                                    if g.get_history('head')[0] - g.head > 0: # Only applied to decreasing heads!
                                        diff += g.get_history('head')[0] - g.head
                                        count += 1
                                        # if g.get_history('head')[0] - g.head < -10:
                                        #     print g.name
                                        #     print g.get_history('head')[0] - g.head
                    try:
                        diff /= count
                    except ZeroDivisionError:
                        diff = 0

            diff_calc = (self.drains[(self.drains.BASIN_NAME==basin)]['head'] - diff - self.drains[(self.drains.BASIN_NAME==basin)]['elev']) *\
                self.drains[(self.drains.BASIN_NAME==basin)]['cond']
            self.target.drain_flow[basin] += diff_calc[(diff_calc > 0)].sum()