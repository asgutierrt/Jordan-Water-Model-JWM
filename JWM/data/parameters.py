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

"""This module defines the Parameters class.

Attributes:
    parameter_inputs_xlsx (pandas ExcelFile object): Pandas ExcelFile object of parameter_inputs.xlsx

"""

from JWM import get_excel_data
import logging

parameter_inputs_xlsx = get_excel_data("parameter_inputs.xlsx")

class Parameters(object):

    def __init__(self, csf_job, simulation_type, simulation_number):
        if (simulation_type != "narrative") and (simulation_type != "sensitivity") and (simulation_type != "tanker"):
            print("+++ User inputs: Simulation type is not narrative, sensitivity, or tanker (model_setup.xlsx) +++")
            simulation_type = "narrative"
            print("+++ ... simulation type set to %s +++" % simulation_type)
        else:
            print("+++ User inputs: Simulation type %s selected (model_setup.xlsx) +++" % simulation_type)
        parameter_file_name = (simulation_type + "_simulation_inputs\\" + "parameter_inputs" + str(simulation_number) +
                               ".xlsx")
        logging.info("+++ Parameter file: " + simulation_type + " " + str(simulation_number) + " +++")

        parameter_inputs_xlsx = get_excel_data(parameter_file_name)
        for agent in parameter_inputs_xlsx.sheet_names[0:]:
            if agent[0:3] == 'ind':
                continue
            if agent.find('ts_') == 0:
                continue
            
            setattr(self, agent, {})
            inputs = parameter_inputs_xlsx.parse(agent)
            for parameter in inputs['parameter_name'].values:
              getattr(self, agent)[parameter] = inputs[(inputs.parameter_name == parameter)]['value'].values[0]

        hh_tariffs = parameter_inputs_xlsx.parse("ind_hh_tariff_structures")[0:9999]
        co_tariffs = parameter_inputs_xlsx.parse("ind_co_tariff_structures")[0:9999]
        ssp_scenario_selected = self.human['ssp_selection']
        annual_tariff_factors = parameter_inputs_xlsx.parse("ind_input_tariff_growth")
        annual_tariff_factors_ssp = \
            annual_tariff_factors.loc[(annual_tariff_factors["scenario"] == ("SSP" + str(ssp_scenario_selected))), :]
        annual_nrw_reduction = parameter_inputs_xlsx.parse("ind_nrw_reduction")
        annual_nrw_reduction_ssp = \
            annual_nrw_reduction.loc[(annual_nrw_reduction["scenario"] == ("SSP" + str(ssp_scenario_selected))), :]
        supply_duration_factors = parameter_inputs_xlsx.parse("ind_supply_duration_factors")
        storage_factors = parameter_inputs_xlsx.parse("ind_storage_factors")
        remigration_factors = parameter_inputs_xlsx.parse("ind_remigration_factors")
        setattr(self, "hh_tariffs", hh_tariffs)
        setattr(self, "co_tariffs", co_tariffs)
        setattr(self, "annual_tariff_factors", annual_tariff_factors_ssp)
        setattr(self, "annual_nrw_reduction", annual_nrw_reduction_ssp)
        setattr(self, "supply_duration_factors", supply_duration_factors)
        setattr(self, "storage_factors", storage_factors)
        setattr(self, "remigration_factors", remigration_factors)


