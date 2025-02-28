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
import logging
log = logging.getLogger('farmer_module')
# from jordanprototype.farmer_diagnostics import write_farmer_properties

class HighlandFarmerConsumptionEngine(Engine):
    name = """Highland Farmers determine groundwater pumping / purchase / water application"""

    def run(self):
        """
            The target of this is an institution containing all Highland farmer nodes
        """
        highland_farms = self.target.nodes
        # Methods which run every month#

        for farm in highland_farms:
            if farm.first_planning_run == True:
                farm.det_irrig_demand()
                farm.det_irrig_supply()

                # farm.det_revised_tanker_alloc()
                farm.det_tanker_supply()

                farm.det_groundwater_abstraction()
                farm.set_groundwater_pumping()

                farm.det_farmer_profits()


            else:
                pass
                #log.info('Farmer planning engine yet to run')