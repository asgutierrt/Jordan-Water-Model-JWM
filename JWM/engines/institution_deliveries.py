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

class CWADeliveryEngine(Engine):
    name = """CWA provides transfers, deliveries to JVA and WAJ"""

    def run(self):
        """
            The target of this is the CWA institution
        """
        self.target.determine_transfers()

class WAJDeliveryEngine(Engine):
    name = """WAJ provides transfers, deliveries to GOVs"""

    def run(self):
        """
            The target of this is the CWA institution
        """

        self.target.determine_gov_delivery(timestep=self.timestep)
        self.target.determine_groundwater_pumping(timestep=self.timestep)

class WAJLifelineEngine(Engine):
    name = """WAJ provides lifeline supply to meet minimum supply threshold"""

    def run(self):
        self.target.determine_lifeline_supply(timestep=self.timestep)


class HumanAgentWrapperEngine(Engine):
    name = """Human Agent Wrapper allocates piped water to household and commercial agents"""

    def run(self):
        """
            The target of this is the Human Agent Wrapper Institution
        """
        self.target.set_industrial_well_and_surface_water_consumption()
        self.target.det_piped_water_demands() 
        self.target.set_final_piped_water_supplies() 
