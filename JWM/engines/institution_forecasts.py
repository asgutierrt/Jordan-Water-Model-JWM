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

class CWAForecastEngine(Engine):
    name = """CWA provides updated forecast to JVA and WAJ"""
    
    def run(self):
        """
            The target of this is the CWA institution
        """
        if self.target.network.parameters.cwa['cwa_dynamic'] == 1:
            self.target.set_allocation()
            self.target.det_wehdah_to_kac_forecast()
        self.target.det_kac_to_zai_forecast()


class JVAMujibForecastEngine(Engine):
    name = """JVA provides updated forecast to WAJ"""

    def run(self):
        """
            The target of this is the JVA institution
        """
        self.target.set_mujib_forecast()
        self.target.set_walah_forecast()


class JVAValleyForecastEngine(Engine):
    name = """JVA provides updated forecast for SO deliveries"""

    def run(self):
        """
            The target of this is the JVA institution
        """
        if self.target.network.parameters.jva['jva_allocation'] == 1:
            self.target.det_demand_forecast()
            self.target.set_allocation_forecast()

class WAJForecastEngine(Engine):
    name = """WAJ provides updated forecast to GOV/utilities"""
    
    def run(self):
        """
            The target of this is the WAJ institution
        """
        if self.timestep.month == 1:
            self.target.set_yearly_max_groundwater_pumping(timestep=self.timestep)
            self.target.set_demand_forecast(timestep=self.timestep)
            self.target.set_allocation_forecast(timestep=self.timestep)
            if self.target.solver_status != 'ok':
                self.target.set_allocation_forecast(timestep=self.timestep)


class GOVForecastEngine(Engine):
    name = """GOV provides supply hours for each urban sub-district"""
    
    def run(self):
        """
            The target of this is an institution containing all gov institutions
        """
        for gov in self.target.institutions:
            gov.estimate_subdist_demand()
            gov.determine_supply_hours_forecast()
