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

from pynsim import Network

class JordanNetwork(Network):

    _properties = {
        'drain_flow': {},
    }
    projection="28191"
    
    def setup(self, timestep):

        print("Progress: timestep " + str(self.timesteps.index(timestep)+1) + ", year: " + str(timestep.year) +
              ", month: " + str(timestep.month))

        self.exogenous_inputs.setup(timestep)
        self.hh_rep_units = {}
        self.hh_init_rep_units = {}

        if self.current_timestep_idx == 0:
            self.subdist_gov_dict = {}
            for hh in self.get_institution('human_agent_wrapper').hh_agents:
                if hh.name[9:15] not in self.subdist_gov_dict.keys():
                    self.subdist_gov_dict[hh.name[9:15]] = hh.gov_name
