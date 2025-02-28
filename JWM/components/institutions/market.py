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

from JWM.components.institutions.government import JordanInstitution
from JWM import get_excel_data
from math import log
import numpy as np
from pyomo.environ import *
from pyomo.opt import SolverFactory
import os
import pickle
import pyutilib.common
from JWM import basepath, excel_data_folder
import logging


class HumanAgentWrapper(JordanInstitution):
    """A Jordan-wide human module institution class."""
    description = "Human Agent Wrapper"

    def __init__(self, name, **kwargs):
        super(HumanAgentWrapper, self).__init__(name, **kwargs)
        self.agents = []
        self.agents_no_industry = []
        self.hh_agents = []
        self.co_agents = []
        self.rf_agents = []
        self.in_agents = []
        self.govs = []
        self.subdists = []

    _properties = {
        'total_in_well_demand': 0,
        'total_in_well_consumption': 0,
        'total_rf_well_demand': 0,
        'total_rf_well_consumption': 0,
        'total_in_surface_demand': 0,
        'total_in_surface_consumption': 0,
        'total_hh_piped_demand': 0,
        'total_co_piped_demand': 0,
        'total_rf_piped_demand': 0,
        'total_in_piped_demand': 0,
        'total_piped_demand': 0,
        'total_hh_piped_consumption': 0,
        'total_co_piped_consumption': 0,
        'total_rf_piped_consumption': 0,
        'total_in_piped_consumption': 0,
        'total_piped_consumption': 0,
        'total_piped_water_remainder': 0,
        'total_hh_tanker_consumption': 0,
        'total_co_tanker_consumption': 0,
        'total_rf_tanker_consumption': 0,
        'total_tanker_consumption': 0,
        'total_hh_consumer_surplus_diff': 0,
        'total_co_consumer_surplus_diff': 0,
        'total_rf_consumer_surplus_diff': 0,
        'total_in_water_value_lost': 0,
        'total_consumer_surplus_diff': 0,
        'total_hh_expenditure': 0,
        'total_co_expenditure': 0,
        'total_rf_expenditure': 0,
        'total_in_expenditure': 0,
        'total_expenditure': 0,
        'countrywide_avg_tanker_price': 0
    }

    def set_industrial_well_and_surface_water_consumption(self):
        month = self.network.current_timestep.month
        total_in_well_demand = 0.0
        total_in_well_consumption = 0.0
        total_rf_well_demand = 0.0
        total_rf_well_consumption = 0.0
        total_in_surface_demand = 0.0
        total_in_surface_consumption = 0.0
        for g in self.govs:
            g.set_industrial_well_and_surface_water_consumption(month)
            total_in_well_demand += g.total_in_well_demand
            total_in_well_consumption += g.total_in_well_consumption
            total_rf_well_demand += g.total_rf_well_demand
            total_rf_well_consumption += g.total_rf_well_consumption
            total_in_surface_demand += g.total_in_surface_demand
            total_in_surface_consumption += g.total_in_surface_consumption
        self.total_in_well_demand = total_in_well_demand
        self.total_in_well_consumption = total_in_well_consumption
        self.total_rf_well_demand = total_rf_well_demand
        self.total_rf_well_consumption = total_rf_well_consumption
        self.total_in_surface_demand = total_in_surface_demand
        self.total_in_surface_consumption = total_in_surface_consumption

    def det_piped_water_demands(self):
        month = self.network.current_timestep.month
        total_hh_piped_demand = 0.0
        total_co_piped_demand = 0.0
        total_rf_piped_demand = 0.0
        total_in_piped_demand = 0.0
        for g in self.govs:
            g.det_piped_water_demands(month)
            total_hh_piped_demand += g.total_hh_piped_demand
            total_co_piped_demand += g.total_co_piped_demand
            total_rf_piped_demand += g.total_rf_piped_demand
            total_in_piped_demand += g.total_in_piped_demand
        self.total_hh_piped_demand = total_hh_piped_demand
        self.total_co_piped_demand = total_co_piped_demand
        self.total_rf_piped_demand = total_rf_piped_demand
        self.total_in_piped_demand = total_in_piped_demand
        self.total_piped_demand = total_hh_piped_demand + total_co_piped_demand + total_rf_piped_demand + \
                                  total_in_piped_demand

    def set_final_piped_water_supplies(self):
        year = self.network.current_timestep.year
        total_hh_piped_consumption = 0.0
        total_co_piped_consumption = 0.0
        total_rf_piped_consumption = 0.0
        total_in_piped_consumption = 0.0
        total_piped_water_remainder = 0.0
        for g in self.govs:
            g.set_final_piped_water_supplies(year)
            total_hh_piped_consumption += g.total_hh_piped_consumption
            total_co_piped_consumption += g.total_co_piped_consumption
            total_rf_piped_consumption += g.total_rf_piped_consumption
            total_in_piped_consumption += g.total_in_piped_consumption
            total_piped_water_remainder += g.total_piped_water_remainder
        self.total_hh_piped_consumption = total_hh_piped_consumption
        self.total_co_piped_consumption = total_co_piped_consumption
        self.total_rf_piped_consumption = total_rf_piped_consumption
        self.total_in_piped_consumption = total_in_piped_consumption
        self.total_piped_consumption = total_hh_piped_consumption + total_co_piped_consumption + \
                                       total_rf_piped_consumption + total_in_piped_consumption
        self.total_piped_water_remainder = total_piped_water_remainder

    def det_tanker_water_consumption(self):
        total_hh_tanker_consumption = 0.0
        total_co_tanker_consumption = 0.0
        total_rf_tanker_consumption = 0.0
        for g in self.govs:
            total_hh_tanker_consumption += g.total_hh_tanker_consumption
            total_co_tanker_consumption += g.total_co_tanker_consumption
            total_rf_tanker_consumption += g.total_rf_tanker_consumption
        self.total_hh_tanker_consumption = total_hh_tanker_consumption
        self.total_co_tanker_consumption = total_co_tanker_consumption
        self.total_rf_tanker_consumption = total_rf_tanker_consumption
        self.total_tanker_consumption = total_hh_tanker_consumption + total_co_tanker_consumption + \
                                        total_rf_tanker_consumption

    def det_human_module_metrics(self):
        total_hh_piped_consumption = 0.0
        total_co_piped_consumption = 0.0
        total_rf_piped_consumption = 0.0
        total_in_piped_consumption = 0.0

        total_hh_expenditure = 0.0
        total_co_expenditure = 0.0
        total_rf_expenditure = 0.0
        total_in_expenditure = 0.0
        total_hh_consumer_surplus_diff = 0.0
        total_co_consumer_surplus_diff = 0.0
        total_rf_consumer_surplus_diff = 0.0
        total_in_water_value_lost = 0.0
        for g in self.govs:
            g.det_expenditure()
            g.det_consumer_surplus_diff()
            g.update_piped_consumption()
            total_hh_piped_consumption += g.total_hh_piped_consumption
            total_co_piped_consumption += g.total_co_piped_consumption
            total_rf_piped_consumption += g.total_rf_piped_consumption
            total_in_piped_consumption += g.total_in_piped_consumption
            total_hh_expenditure += g.total_hh_expenditure
            total_co_expenditure += g.total_co_expenditure
            total_rf_expenditure += g.total_rf_expenditure
            total_in_expenditure += g.total_in_expenditure
            total_hh_consumer_surplus_diff += g.total_hh_consumer_surplus_diff
            total_co_consumer_surplus_diff += g.total_co_consumer_surplus_diff
            total_rf_consumer_surplus_diff += g.total_rf_consumer_surplus_diff
            total_in_water_value_lost += g.total_in_water_value_lost
        self.total_hh_piped_consumption = total_hh_piped_consumption
        self.total_co_piped_consumption = total_co_piped_consumption
        self.total_rf_piped_consumption = total_rf_piped_consumption
        self.total_in_piped_consumption = total_in_piped_consumption
        self.total_piped_consumption = total_hh_piped_consumption + total_co_piped_consumption + \
                                       total_rf_piped_consumption + total_in_piped_consumption
        self.total_hh_expenditure = total_hh_expenditure
        self.total_co_expenditure = total_co_expenditure
        self.total_rf_expenditure = total_rf_expenditure
        self.total_in_expenditure = total_in_expenditure
        self.total_expenditure = total_hh_expenditure + total_co_expenditure + total_rf_expenditure + \
                                 total_in_expenditure
        self.total_hh_consumer_surplus_diff = total_hh_consumer_surplus_diff
        self.total_co_consumer_surplus_diff = total_co_consumer_surplus_diff
        self.total_rf_consumer_surplus_diff = total_rf_consumer_surplus_diff
        self.total_in_water_value_lost = total_in_water_value_lost
        self.total_consumer_surplus_diff = total_hh_consumer_surplus_diff + total_co_consumer_surplus_diff + \
                                           total_rf_consumer_surplus_diff + total_in_water_value_lost
        self.print_human_module_metrics_to_console()

    def print_human_module_metrics_to_console(self):
        month = self.network.current_timestep.month
        detailed_logging = 1
        if (month == 12) & (detailed_logging == 1):
            logging.info("\n****** 1. Piped water demands: ******")
            for g in sorted(self.govs, key=lambda x: x.name):
                logging.info(g.name.split("_")[0] + ": Piped demand (m3/y), households: " +
                     str(sum(g.get_history("total_hh_piped_demand")[-11:]) + g.total_hh_piped_demand) +
                     " , commercials: " +
                     str(sum(g.get_history("total_co_piped_demand")[-11:]) + g.total_co_piped_demand) +
                     " , refugees: " +
                     str(sum(g.get_history("total_rf_piped_demand")[-11:]) + g.total_rf_piped_demand) +
                     " , industry: " +
                     str(sum(g.get_history("total_in_piped_demand")[-11:]) + g.total_in_piped_demand))
            logging.info("JORDAN: Piped demand (m3/y), households: " +
                 str(sum(self.get_history("total_hh_piped_demand")[-11:]) + self.total_hh_piped_demand) +
                 " , commercials: " +
                 str(sum(self.get_history("total_co_piped_demand")[-11:]) + self.total_co_piped_demand) +
                 " , refugees: " +
                 str(sum(self.get_history("total_rf_piped_demand")[-11:]) + self.total_rf_piped_demand) +
                 " , industry: " +
                 str(sum(self.get_history("total_in_piped_demand")[-11:]) + self.total_in_piped_demand))
            logging.info("\n****** 2. Piped water consumption: ******")
            for g in sorted(self.govs, key=lambda x: x.name):
                logging.info(g.name.split("_")[0] + ": Piped consumption (m3/y), households: " +
                     str(sum(g.get_history("total_hh_piped_consumption")[-11:]) + g.total_hh_piped_consumption) +
                     " , commercials: " +
                     str(sum(g.get_history("total_co_piped_consumption")[-11:]) + g.total_co_piped_consumption) +
                     " , refugees: " +
                     str(sum(g.get_history("total_rf_piped_consumption")[-11:]) + g.total_rf_piped_consumption) +
                     " , industry: " +
                     str(sum(g.get_history("total_in_piped_consumption")[-11:]) + g.total_in_piped_consumption))
            logging.info("JORDAN: Piped consumption (m3/y), households: " +
                 str(sum(self.get_history("total_hh_piped_consumption")[-11:]) + self.total_hh_piped_consumption) +
                 " , commercials: " +
                 str(sum(self.get_history("total_co_piped_consumption")[-11:]) + self.total_co_piped_consumption) +
                 " , refugees: " +
                 str(sum(self.get_history("total_rf_piped_consumption")[-11:]) + self.total_rf_piped_consumption) +
                 " , industry: " +
                 str(sum(self.get_history("total_in_piped_consumption")[-11:]) + self.total_in_piped_consumption))
            logging.info("\n****** 3. Tanker water consumption: ******")
            for g in sorted(self.govs, key=lambda x: x.name):
                logging.info(g.name.split("_")[0] + ": Household consumption (m3/y): " +
                     str(sum(g.get_history("total_hh_tanker_consumption")[-11:]) + g.total_hh_tanker_consumption) +
                     " , commercial consumption (m3/y): " +
                     str(sum(g.get_history("total_co_tanker_consumption")[-11:]) + g.total_co_tanker_consumption) +
                     " , refugee consumption (m3/y): " +
                     str(sum(g.get_history("total_rf_tanker_consumption")[-11:]) + g.total_rf_tanker_consumption))
            logging.info("JORDAN: Household consumption (m3/y): " +
                str(sum(self.get_history("total_hh_tanker_consumption")[-11:]) + self.total_hh_tanker_consumption) +
                " , commercial consumption (m3/y): " +
                str(sum(self.get_history("total_co_tanker_consumption")[-11:]) + self.total_co_tanker_consumption) +
                " , refugee consumption (m3/y): " +
                str(sum(self.get_history("total_rf_tanker_consumption")[-11:]) + self.total_rf_tanker_consumption))
            cummulative_tanker_price = 0
            cummulative_tanker_qties = 0
            for a in self.agents_no_industry:
                cummulative_tanker_price += (sum(a.get_history("final_tanker_price")[-11:]) + a.final_tanker_price) *\
                                            (sum(a.get_history("tanker_consumption")[-11:]) + a.tanker_consumption) *\
                                            a.represented_units / (12.0 * 12.0)
                cummulative_tanker_qties += (sum(a.get_history("tanker_consumption")[-11:]) + a.tanker_consumption) *\
                                            a.represented_units / 12.0
            if cummulative_tanker_qties > 0.0:
                self.countrywide_avg_tanker_price = cummulative_tanker_price / cummulative_tanker_qties #JIM added for metric calculation
                logging.info("JORDAN: Avg. tanker price (JD/m3): " + str(self.countrywide_avg_tanker_price))
            logging.info("\n****** 4. Expenditure and Consumer Surplus: ******")
            logging.info("JORDAN: Total expenditure for piped and tanker water:                           " +
                         str(sum(self.get_history("total_expenditure")[-11:]) + self.total_expenditure))

    def write_baseline_pickle_files(self, year=2006):
        start_month=int((year-self.network.timesteps[0].year)*12)
        end_month=start_month+12

        masterlist = {}
        for a in self.agents_no_industry:
            masterlist[a.name] = []
            for t in range(start_month, end_month):
                calc = a.get_history('piped_consumption')[t] + a.get_history('tanker_consumption')[t]
                preserved = repr(calc)
                masterlist[a.name].append(preserved)
        filename = "baseline_consumption.p"
        data_file = os.path.join(basepath, excel_data_folder, filename)
        outfile = file(data_file, "w")
        pickle.dump(masterlist, outfile)
        outfile.close()

        masterlist = {}
        for a in self.agents_no_industry:
            masterlist[a.name] = []
            for t in range(start_month, end_month):
                calc = a.get_history('expenditure')[t]
                preserved = repr(calc)
                masterlist[a.name].append(preserved)
        filename = "baseline_expenditure.p"
        data_file = os.path.join(basepath, excel_data_folder, filename)
        outfile = file(data_file, "w")
        pickle.dump(masterlist, outfile)
        outfile.close()

        masterlist = {}
        for a in self.agents_no_industry:
            masterlist[a.name] = []
            for t in range(start_month, end_month):
                calc = a.get_history('represented_units')[t]
                preserved = repr(calc)
                masterlist[a.name].append(preserved)
        filename = "baseline_represented_units.p"
        data_file = os.path.join(basepath, excel_data_folder, filename)
        outfile = file(data_file, "w")
        pickle.dump(masterlist, outfile)
        outfile.close()

    def setup(self, timestamp):
        pass


class GovAgentWrapper(JordanInstitution):
    """A governorate-level human module institution class."""
    description = "Gov Agent Wrapper"

    def __init__(self, name, **kwargs):
        super(GovAgentWrapper, self).__init__(name, **kwargs)
        self.gov_id = 0
        self.x = 0
        self.y = 0
        self.agents = []
        self.agents_no_industry = []
        self.hh_agents = []
        self.co_agents = []
        self.rf_agents = []
        self.in_agents = []
        self.gov_name = self.name[0:3]
        self.tariff_blocks = {'sewage':[[], [], [], [], [], []],
                              'nonsewage':[[], [], [], [], [], []]}
        self.hh_tariff_structures = None

        self.total_nrw_share_base = 0
        self.admin_nrw_share_base = 0
        self.tech_nrw_share_base = 0
        self.total_nrw_share_admin_base = 0
        self.admin_nrw_share_after_tech_nrw_base = 0

        self.total_nrw_share = 0
        self.admin_nrw_share = 0
        self.tech_nrw_share = 0
        self.admin_nrw_share_after_tech_nrw = 0

        self.WAJ_inputs = None

    _properties = {
        'total_in_well_demand': 0,
        'total_in_well_consumption': 0,
        'total_rf_well_demand': 0,
        'total_rf_well_consumption': 0,
        'total_in_surface_demand': 0,
        'total_in_surface_consumption': 0,
        'total_hh_piped_demand': 0,
        'total_co_piped_demand': 0,
        'total_rf_piped_demand': 0,
        'total_in_piped_demand': 0,
        'total_piped_demand': 0,
        'total_hh_piped_consumption': 0,
        'total_co_piped_consumption': 0,
        'total_rf_piped_consumption': 0,
        'total_in_piped_consumption': 0,
        'total_piped_consumption': 0,
        'total_piped_water_remainder': 0,
        'total_hh_tanker_consumption': 0,
        'total_co_tanker_consumption': 0,
        'total_rf_tanker_consumption': 0,
        'total_tanker_consumption': 0,
        'total_hh_consumer_surplus_diff': 0,
        'total_co_consumer_surplus_diff': 0,
        'total_rf_consumer_surplus_diff': 0,
        'total_in_water_value_lost': 0,
        'total_consumer_surplus_diff': 0,
        'total_hh_expenditure': 0,
        'total_co_expenditure': 0,
        'total_rf_expenditure': 0,
        'total_in_expenditure': 0,
        'total_expenditure': 0,
        'waj_delivery': 0
    }

    def _get_hh_tariff_structures(self):
        """
            Get the household tariff structure from the node's network object.
            Only need to do this once.
        """
        if self.hh_tariff_structures is None:
            self.hh_tariff_structures = self.network.parameters.hh_tariffs
            self.hh_tariff_changeyears     = list(self.hh_tariff_structures["change_years"])
            self.hh_tariff_yeargovcodes    = list(self.hh_tariff_structures["year&gov_code"])
            self.hh_tariff_numbers         = list(self.hh_tariff_structures["tariff_no"])
            self.hh_tariff_codes           = list(self.hh_tariff_structures["tariff_code"])
            self.hh_tariff_isdemandupdates =  list(self.hh_tariff_structures["is_demand_update"])

    def update_tariff_structure(self, year):
        self._get_hh_tariff_structures()
        tariff_factor_base = self.network.parameters.annual_tariff_factors.loc[
            (self.network.parameters.annual_tariff_factors["year"] == year),
            "household_tariff_factor"
        ].values[0]
        tariff_inflation_correction = self.network.exogenous_inputs.all_tariff_inflation_correction.loc[
            (self.network.exogenous_inputs.all_tariff_inflation_correction["year"] == year),
            "tariff_inflation_correction"
        ].values[0]
        tariff_factor = tariff_factor_base * tariff_inflation_correction

        tariff_year_code = 1
        for i in self.hh_tariff_changeyears:
            if year >= i:
                tariff_year_code += 1

        tariff_index =  self.hh_tariff_yeargovcodes.index((100 * tariff_year_code) + self.gov_id)
        tariff_codes = [i for i, x in enumerate(self.hh_tariff_numbers)
                            if x == ((10*tariff_year_code) + self.hh_tariff_codes
                            [tariff_index])]

        tariff_blocks = [self.hh_tariff_structures.iloc[tariff_codes, i].tolist() for i in range(7, 18)]  # RSP

        is_demand_update = self.hh_tariff_isdemandupdates[tariff_index]

        tariff_blocks.append(is_demand_update)

        self.tariff_blocks['sewage'][0] = tariff_blocks[0]
        self.tariff_blocks['sewage'][1] = [(a + b) * tariff_factor for a, b in zip(tariff_blocks[1], tariff_blocks[3])]
        self.tariff_blocks['sewage'][2] = [(a + b) * tariff_factor for a, b in zip(tariff_blocks[2], tariff_blocks[4])]
        self.tariff_blocks['sewage'][3] = [(a + b) * tariff_factor for a, b in zip(tariff_blocks[5], tariff_blocks[6])]
        self.tariff_blocks['sewage'][4] = [(a + b) * tariff_factor for a, b in zip(tariff_blocks[7], tariff_blocks[8])]
        self.tariff_blocks['sewage'][5] = [(a + b) * tariff_factor for a, b in zip(tariff_blocks[9], tariff_blocks[10])]

        self.tariff_blocks['nonsewage'][0] = tariff_blocks[0]
        self.tariff_blocks['nonsewage'][1] = [a * tariff_factor for a in tariff_blocks[1]]
        self.tariff_blocks['nonsewage'][2] = [a * tariff_factor for a in tariff_blocks[2]]
        self.tariff_blocks['nonsewage'][3] = [a * tariff_factor for a in tariff_blocks[5]]
        self.tariff_blocks['nonsewage'][4] = [a * tariff_factor for a in tariff_blocks[7]]
        self.tariff_blocks['nonsewage'][5] = [a * tariff_factor for a in tariff_blocks[9]]

    def update_nrw_shares(self, year):
        admin_nrw_reduction_factor = self.network.parameters.annual_nrw_reduction.loc[
            (self.network.parameters.annual_nrw_reduction["year"] == year),
            "admin_nrw_reduction"
        ].values[0]
        tech_nrw_reduction_factor = self.network.parameters.annual_nrw_reduction.loc[
            (self.network.parameters.annual_nrw_reduction["year"] == year),
            "tech_nrw_reduction"
        ].values[0]

        ################################################################
        # Tanker analysis investment 2: Equitable distribution - START #
        ################################################################
        if self.network.simulation_type == "tanker":
            if self.network.parameters.tanker['tanker_analysis_investment_2'] == 1:
                adjustment_speed = 0.5
                haw = self.network.get_institution("human_agent_wrapper")
                ts_now1 = self.network.current_timestep_idx
                if ts_now1 <= 11:
                    self.network.parameters.nrw_policy_factor = 1.0
                    self.network.parameters.nrw_policy_adjustment = 1.0
                    self.network.parameters.nrw_policy_factor_history = np.repeat(1.0, 12).tolist()
                    self.network.parameters.nrw_deviation_history = np.repeat(0.0, 12).tolist()
                    self.network.parameters.misc['supply_hours_equal'] == 'no'
                    self.network.parameters.equal_supply_triggered = False
                    self.network.parameters.last_update_ts = 11
                    self.network.parameters.equal_supply_timestep = None
                else:
                    if self.network.parameters.last_update_ts < ts_now1:
                        last_target = np.linspace(58560000., 58560000., 36 * 12)[ts_now1 - 1]
                        past_years = self.network.current_timestep.year - 2015
                        last_month = self.network.current_timestep.month - 2
                        past_last_months = [((i * 12) + last_month) for i in range(past_years)]
                        past_all_months = [i for i in range(past_years * 12)]
                        sales_sum_past_years = sum(haw.get_history("total_tanker_consumption")[i] for i in past_all_months)
                        sales_sum_past_last_months = sum(
                            haw.get_history("total_tanker_consumption")[i] for i in past_last_months)
                        sales_share_past_avg_last_month = sales_sum_past_last_months / sales_sum_past_years
                        last_month_target = last_target * sales_share_past_avg_last_month
                        last_month_sales = haw.get_history("total_tanker_consumption")[-1]
                        deviation_pct = (last_month_sales / last_month_target) - 1.0
                        nrw_policy_adjustment = (self.network.parameters.nrw_policy_adjustment -
                                                 min(0.05, (deviation_pct * adjustment_speed)))
                        self.network.parameters.nrw_policy_adjustment = min(1.0, max(0.0, nrw_policy_adjustment))
                        self.network.parameters.nrw_policy_factor = min(1.0, max(0.0, nrw_policy_adjustment))
                        if (not self.network.parameters.equal_supply_triggered and
                                self.network.parameters.nrw_policy_factor < 0.9):
                            self.network.parameters.equal_supply_triggered = True
                            self.network.parameters.misc['supply_hours_equal'] = 'yes'
                            self.network.parameters.nrw_policy_adjustment = 0.95
                            self.network.parameters.nrw_policy_factor = 0.95
                            self.network.parameters.equal_supply_timestep = ts_now1
                        self.network.parameters.nrw_policy_factor_history += [self.network.parameters.nrw_policy_factor]
                        self.network.parameters.nrw_deviation_history += [deviation_pct]
                        print("Tanker water market analyses - improved public water distribution:")
                        equalization_stage_message = ("equalized" if self.network.parameters.equal_supply_triggered
                                                      else "pre-equalization")
                        print(" - NRW reduction factor: " + str(self.network.parameters.nrw_policy_factor))
                        print(" - Supply equalization stage: " + equalization_stage_message)
                        self.network.parameters.last_update_ts = ts_now1
                nrw_policy_factor = self.network.parameters.nrw_policy_factor
                admin_nrw_reduction_factor = 1.0
                tech_nrw_reduction_factor = nrw_policy_factor
        ##############################################################
        # Tanker analysis investment 2: Equitable distribution - END #
        ##############################################################

        year_max_2015 = year
        if year_max_2015 > 2015:
            year_max_2015 = 2015

        self.total_nrw_share_base = self.network.exogenous_inputs.human_nrw[
            ((year_max_2015 - 2006) * 12) + self.gov_id - 1]
        self.admin_nrw_share_base = 0.5 * self.total_nrw_share_base
        self.tech_nrw_share_base = 0.5 * self.total_nrw_share_base
        self.admin_nrw_share = self.admin_nrw_share_base * admin_nrw_reduction_factor
        self.tech_nrw_share = self.tech_nrw_share_base * tech_nrw_reduction_factor
        self.total_nrw_share = self.admin_nrw_share + self.tech_nrw_share
        self.total_nrw_share_admin_base = self.admin_nrw_share_base + self.tech_nrw_share
        self.admin_nrw_share_after_tech_nrw_base = 1 - ((1 - self.total_nrw_share_admin_base) /
                                                        (1 - self.tech_nrw_share))
        self.admin_nrw_share_after_tech_nrw = 1 - ((1 - self.total_nrw_share) /
                                                   (1 - self.tech_nrw_share))

    def set_industrial_well_and_surface_water_consumption(self, month):
        total_in_well_demand = 0.0
        total_in_well_consumption = 0.0
        total_rf_well_demand = 0.0
        total_rf_well_consumption = 0.0
        total_in_surface_demand = 0.0
        total_in_surface_consumption = 0.0

        for a in self.in_agents:
            a.total_demand = 0.0
            a.total_consumption = 0.0
            a.expenditure = 0.0

            a.det_well_water_demand(month)
            a.total_demand += a.well_demand
            total_in_well_demand += a.well_demand * 365.0 / 12.0
            a.well_consumption = a.well_demand
            a.total_consumption += a.well_consumption
            a.well_expenditure = a.groundwater_cost * a.well_consumption
            a.expenditure += a.well_expenditure
            total_in_well_consumption += a.well_consumption * 365.0 / 12.0

            a.det_surface_water_demand(month)
            a.total_demand += a.surface_demand
            total_in_surface_demand += a.surface_demand * 365.0 / 12.0
            a.surface_consumption = a.surface_demand
            a.total_consumption += a.surface_consumption
            a.surface_expenditure = a.surface_water_cost * a.surface_consumption
            a.expenditure += a.surface_expenditure
            total_in_surface_consumption += a.surface_consumption * 365.0 / 12.0

        for a in [x for x in self.rf_agents if (x.is_camp_location and x.camp_groundwater_use > 0)]:
            a.det_well_water_demand()
            total_rf_well_demand += a.camp_groundwater_demand * 365.0 / 12.0
            a.camp_groundwater_consumption = a.camp_groundwater_demand
            total_rf_well_consumption += a.camp_groundwater_consumption * 365.0 / 12.0

        self.total_in_well_demand = total_in_well_demand
        self.total_in_well_consumption = total_in_well_consumption
        self.total_rf_well_demand = total_rf_well_demand
        self.total_rf_well_consumption = total_rf_well_consumption
        self.total_in_surface_demand = total_in_surface_demand
        self.total_in_surface_consumption = total_in_surface_consumption

    def det_piped_water_demands(self, month):
        total_hh_piped_demand = 0.0
        total_co_piped_demand = 0.0
        total_rf_piped_demand = 0.0
        total_in_piped_demand = 0.0
        for a in self.hh_agents:
            a.det_piped_water_demand(month)
            total_hh_piped_demand += a.piped_demand * a.represented_units * 365.0 / 12.0
        for a in self.co_agents:
            a.det_piped_water_demand(month)
            total_co_piped_demand += a.piped_demand * a.represented_units * 365.0 / 12.0
        for a in self.rf_agents:
            a.det_piped_water_demand(month)
            total_rf_piped_demand += a.piped_demand * a.represented_units * 365.0 / 12.0
        for a in self.in_agents:
            a.det_piped_water_demand(month)
            a.total_demand += a.piped_demand
            total_in_piped_demand += a.piped_demand * 365.0 / 12.0
        self.total_hh_piped_demand = total_hh_piped_demand
        self.total_co_piped_demand = total_co_piped_demand
        self.total_rf_piped_demand = total_rf_piped_demand
        self.total_in_piped_demand = total_in_piped_demand
        self.total_piped_demand = total_co_piped_demand + total_hh_piped_demand + total_rf_piped_demand + \
                                  total_in_piped_demand

    def set_final_piped_water_supplies(self, year):
        # 1. Reset variables:
        self.total_piped_consumption = 0.0
        self.total_hh_piped_consumption = 0.0
        self.total_co_piped_consumption = 0.0
        self.total_rf_piped_consumption = 0.0
        self.total_in_piped_consumption = 0.0

        # 2. Get WAJ delivery:
        gov_name = self.name.split("_")[0]

        self.waj_delivery = self.network.get_institution('waj').gov_delivery[gov_name] * (1 - self.tech_nrw_share) * \
                            self.network.parameters.waj['waj_gov_factor'] * 12.0 / 365.0

        # 3. Determine industrial piped consumption:
        if (self.total_in_piped_demand * 12.0 / 365.0) <= self.waj_delivery:
            reduction_factor = 1.0
        else:
            reduction_factor = self.waj_delivery / (self.total_in_piped_demand * 12.0 / 365.0)
        for a in self.in_agents:
            a.piped_consumption = reduction_factor * a.piped_demand
            a.total_consumption += a.piped_consumption
            a.piped_expenditure = a.tariff * a.piped_consumption
            a.expenditure += a.piped_expenditure
            self.total_in_piped_consumption += a.piped_consumption * 365.0 / 12.0
            self.total_in_expenditure += a.piped_expenditure * 365.0 / 12.0

        # 4. Determine unconstrained non-industrial piped consumption:
        waj_delivery_no_industry = self.waj_delivery - (self.total_in_piped_consumption * 12.0 / 365.0)
        total_daily_piped_demand_no_industry = 0.0
        for a in self.agents_no_industry:
            a.piped_constraint = False
            supply_day_weight = a.duration / 7.0
            a.piped_non_supply_day_consumption = a.piped_non_supply_day_demand
            a.piped_supply_day_consumption = a.piped_demand
            a.piped_consumption = a.piped_non_supply_day_consumption * (1.0 - supply_day_weight) + \
                                  a.piped_supply_day_consumption * supply_day_weight
            total_daily_piped_demand_no_industry += a.piped_consumption * a.represented_units
            # 4.a. Resetting indicator variable for the case of insufficient supply:
            a.is_non_supply_day_demand_satisfied = False
        self.total_piped_water_remainder = ((waj_delivery_no_industry - total_daily_piped_demand_no_industry) *
                                            365.0 / 12.0)

        # 5. Determine constrained non-industrial piped consumption in case of insufficient supply:
        if self.total_piped_water_remainder < 0.0:
            # 5.1. Setting up key variables for the distribution:
            self.total_piped_water_remainder = 0.0
            satisfied_agents = []
            water_distributed = 0
            unsatisfied_agents = list(a for a in self.agents_no_industry if a.represented_units > 0)
            distribution_completed = False
            # 5.2. Distribution loop to iteratively reallocate water left in the network by satisfied consumer agents:
            while not distribution_completed:
                # 5.2.1. Setting up key variables for the current distribution iteration:
                distribution_completed = True
                remaining_delivery = waj_delivery_no_industry - water_distributed
                total_duration_of_unsatisfied_agents = 0
                # 5.2.2. Determining the sum of supply hours across all agents to prepare delivery share calculation:
                for u in unsatisfied_agents:
                    supply_day_weight = u.duration / 7.0
                    if u.type == "commercial":
                        total_duration_of_unsatisfied_agents += u.duration * u.represented_units * u.connection_size * \
                                                                supply_day_weight
                        if not u.is_non_supply_day_demand_satisfied:
                            total_duration_of_unsatisfied_agents += u.duration * u.represented_units * \
                                                                    u.connection_size * (1.0 - supply_day_weight)
                    else:
                        total_duration_of_unsatisfied_agents += u.duration * u.represented_units * supply_day_weight
                        if not u.is_non_supply_day_demand_satisfied:
                            total_duration_of_unsatisfied_agents += u.duration * u.represented_units * \
                                                                    (1.0 - supply_day_weight)
                # 5.2.3. Determining which agents are satisfied with the current delivery share and which demand more:
                for u in unsatisfied_agents:
                    # 5.2.3.1. Determine delivery share:
                    if u.type == "commercial":
                        delivery_share = u.duration * remaining_delivery * u.connection_size / total_duration_of_unsatisfied_agents # * u.connection_rate \
                    else:
                        delivery_share = u.duration * remaining_delivery / total_duration_of_unsatisfied_agents
                    supply_day_weight = u.duration / 7.0
                    # 5.2.3.2. Outcomes for non-supply-day consumption:
                    if not u.is_non_supply_day_demand_satisfied:
                        if delivery_share <= u.piped_non_supply_day_demand:
                            u.piped_non_supply_day_consumption = delivery_share
                        else:
                            distribution_completed = False
                            u.piped_non_supply_day_consumption = u.piped_non_supply_day_demand
                            u.is_non_supply_day_demand_satisfied = True
                            water_distributed += u.piped_non_supply_day_consumption * \
                                                 u.represented_units * (1.0 - supply_day_weight)
                    # 5.2.3.3. Outcomes for supply-day consumption:
                    if delivery_share <= u.piped_demand:
                        u.piped_supply_day_consumption = delivery_share
                        u.piped_constraint = True
                    else:
                        distribution_completed = False
                        u.piped_supply_day_consumption = u.piped_demand
                        satisfied_agents.append(u)
                        unsatisfied_agents.remove(u)
                        water_distributed += u.piped_supply_day_consumption * u.represented_units * supply_day_weight
                        u.piped_constraint = False
                    # 5.2.3.4. Calculating the weighted-avg. piped consumption for this iteration:
                    u.piped_consumption = u.piped_non_supply_day_consumption * (1.0 - supply_day_weight) + \
                                          u.piped_supply_day_consumption * supply_day_weight

        # 6. Update agent vars by consumer type: (NOTE: Using billed consumption in block choice corrects admin. NRW!):
        for a in self.agents_no_industry:
            # 6.1 Update vars for active agents by consumer type:
            agent_tariff_blocks = a.get_tariff_blocks()
            if a.represented_units > 0:
                billed_piped_consumption = (1 - self.admin_nrw_share_after_tech_nrw) * a.piped_consumption
                if a.type == "household":
                    self.total_hh_piped_consumption += a.piped_consumption * a.represented_units * 365.0 / 12.0
                    for b in range((len(agent_tariff_blocks[0]) - 1), 0, -1):
                        if billed_piped_consumption <= agent_tariff_blocks[0][b] / 91.25:
                            a.tariff_block = b - 1
                            a.tariff = agent_tariff_blocks[1][b - 1] + \
                                       (agent_tariff_blocks[2][b - 1] * a.piped_consumption)
                            a.tariff_fixed_cost = (agent_tariff_blocks[3][b - 1] - agent_tariff_blocks[5][b - 1]) / 91.25
                            if agent_tariff_blocks[2][b - 1] > 0:
                                linear_block_consumption = (billed_piped_consumption - \
                                                            (agent_tariff_blocks[0][b - 1] / 91.25))
                                linear_block_start_tariff = agent_tariff_blocks[1][b - 1] + (
                                    agent_tariff_blocks[2][b - 1] * (agent_tariff_blocks[0][b - 1] / 91.25))
                                linear_block_rsp = (0.5 * linear_block_consumption *
                                                    (a.tariff - linear_block_start_tariff))
                                a.tariff_fixed_cost -= max(0, linear_block_rsp)
                            a.piped_expenditure = a.represented_units * (a.tariff * billed_piped_consumption +
                                                                         a.tariff_fixed_cost)
                            a.expenditure = a.piped_expenditure
                elif a.type == "refugee":
                    self.total_rf_piped_consumption += a.piped_consumption * a.represented_units * 365.0 / 12.0
                    for b in range((len(agent_tariff_blocks[0]) - 1), 0, -1):
                        if billed_piped_consumption <= agent_tariff_blocks[0][b] / 91.25:
                            a.tariff_block = b - 1
                            a.tariff = agent_tariff_blocks[1][b - 1] + \
                                       (agent_tariff_blocks[2][b - 1] * a.piped_consumption)
                            a.tariff_fixed_cost = (agent_tariff_blocks[3][b - 1] - agent_tariff_blocks[5][b - 1]) / 91.25
                            if agent_tariff_blocks[2][b - 1] > 0:
                                linear_block_consumption = (billed_piped_consumption - \
                                                            (agent_tariff_blocks[0][b - 1] / 91.25))
                                linear_block_start_tariff = agent_tariff_blocks[1][b - 1] + (
                                    agent_tariff_blocks[2][b - 1] * (agent_tariff_blocks[0][b - 1] / 91.25))
                                linear_block_rsp = (0.5 * linear_block_consumption *
                                                    (a.tariff - linear_block_start_tariff))
                                a.tariff_fixed_cost -= max(0, linear_block_rsp)
                            a.piped_expenditure = a.represented_units * (a.tariff * billed_piped_consumption +
                                                                         a.tariff_fixed_cost)
                            a.expenditure = a.piped_expenditure
                else:  # For commercial agents:
                    self.total_co_piped_consumption += a.piped_consumption * a.represented_units * 365.0 / 12.0
                    if billed_piped_consumption <= agent_tariff_blocks[0][1] / 91.25:
                        a.tariff_block = 0
                        a.tariff = 0
                        a.rsp = 0
                        a.tariff_fixed_cost = agent_tariff_blocks[2][0] / 91.25
                        a.piped_expenditure = a.represented_units * a.tariff_fixed_cost
                        a.expenditure = a.piped_expenditure
                    else:
                        a.tariff_block = 1
                        a.tariff = agent_tariff_blocks[1]
                        a.rsp = 0
                        a.tariff_fixed_cost = agent_tariff_blocks[2][1] / 91.25
                        a.piped_expenditure = a.represented_units * (a.tariff_fixed_cost + (
                            a.tariff * (billed_piped_consumption - (agent_tariff_blocks[0][1] / 91.25))))
                        a.expenditure = a.piped_expenditure

            # 6.2 Update vars for inactive agents:
            else:
                a.tariff_block = 0
                a.tariff = 0
                a.rsp = 0
                a.tariff_fixed_cost = 0
                a.piped_expenditure = 0
                a.expenditure = a.piped_expenditure

            # 6.3 Update storage constraint:
            a.storage_constraint = False
            if a.piped_non_supply_day_consumption >= (a.storage / max(1e-9, (7.0 - a.duration))):
                a.storage_constraint = True

        # 7. Update total piped consumption:
        self.total_piped_consumption = self.total_hh_piped_consumption + self.total_co_piped_consumption + \
                                       self.total_rf_piped_consumption + self.total_in_piped_consumption

    def det_tanker_water_consumption(self):
        total_hh_tanker_consumption = 0.0
        total_co_tanker_consumption = 0.0
        total_rf_tanker_consumption = 0.0
        for a in self.hh_agents:
            total_hh_tanker_consumption += a.tanker_consumption * a.represented_units * 365.0 / 12.0
        for a in self.co_agents:
            total_co_tanker_consumption += a.tanker_consumption * a.represented_units * 365.0 / 12.0
        for a in self.rf_agents:
            total_rf_tanker_consumption += a.tanker_consumption * a.represented_units * 365.0 / 12.0
        self.total_hh_tanker_consumption = total_hh_tanker_consumption
        self.total_co_tanker_consumption = total_co_tanker_consumption
        self.total_rf_tanker_consumption = total_rf_tanker_consumption
        self.total_tanker_consumption = total_co_tanker_consumption + total_hh_tanker_consumption + \
                                        total_rf_tanker_consumption

    def det_consumer_surplus_diff(self):
        total_hh_consumer_surplus_diff = 0.0
        total_co_consumer_surplus_diff = 0.0
        total_rf_consumer_surplus_diff = 0.0
        total_in_water_value_lost = 0.0
        for a in self.hh_agents:
            a.det_consumer_surplus_diff()
            total_hh_consumer_surplus_diff += a.consumer_surplus_diff * 365.0 / 12.0
        for a in self.co_agents:
            a.det_consumer_surplus_diff()
            total_co_consumer_surplus_diff += a.consumer_surplus_diff * 365.0 / 12.0
        for a in self.rf_agents:
            a.det_consumer_surplus_diff()
            total_rf_consumer_surplus_diff += a.consumer_surplus_diff * 365.0 / 12.0
        for a in self.in_agents:
            a.det_water_value_lost()
            total_in_water_value_lost += a.water_value_lost * 365.0 / 12.0
        self.total_hh_consumer_surplus_diff = total_hh_consumer_surplus_diff
        self.total_co_consumer_surplus_diff = total_co_consumer_surplus_diff
        self.total_rf_consumer_surplus_diff = total_rf_consumer_surplus_diff
        self.total_in_water_value_lost = total_in_water_value_lost
        self.total_consumer_surplus_diff = total_hh_consumer_surplus_diff + total_co_consumer_surplus_diff + \
                                           total_rf_consumer_surplus_diff + total_in_water_value_lost

    def det_expenditure(self):
        total_hh_expenditure = 0.0
        total_co_expenditure = 0.0
        total_rf_expenditure = 0.0
        total_in_expenditure = 0.0
        for a in self.hh_agents:
            total_hh_expenditure += a.expenditure * 365.0 / 12.0
        for a in self.co_agents:
            total_co_expenditure += a.expenditure * 365.0 / 12.0
        for a in self.rf_agents:
            total_rf_expenditure += a.expenditure * 365.0 / 12.0
        for a in self.in_agents:
            total_in_expenditure += a.expenditure * 365.0 / 12.0
        self.total_hh_expenditure = total_hh_expenditure
        self.total_co_expenditure = total_co_expenditure
        self.total_rf_expenditure = total_rf_expenditure
        self.total_in_expenditure = total_in_expenditure
        self.total_expenditure = total_hh_expenditure + total_co_expenditure + total_rf_expenditure + \
                                 total_in_expenditure

    def update_piped_consumption(self):
        total_hh_piped_consumption = 0.0
        total_co_piped_consumption = 0.0
        total_rf_piped_consumption = 0.0
        total_in_piped_consumption = 0.0
        for a in self.hh_agents:
            total_hh_piped_consumption += a.piped_consumption * a.represented_units * 365.0 / 12.0
        for a in self.co_agents:
            total_co_piped_consumption += a.piped_consumption * a.represented_units * 365.0 / 12.0
        for a in self.rf_agents:
            total_rf_piped_consumption += a.piped_consumption * a.represented_units * 365.0 / 12.0
        for a in self.in_agents:
            total_in_piped_consumption += a.piped_consumption * a.represented_units * 365.0 / 12.0
        self.total_hh_piped_consumption = total_hh_piped_consumption
        self.total_co_piped_consumption = total_co_piped_consumption
        self.total_rf_piped_consumption = total_rf_piped_consumption
        self.total_in_piped_consumption = total_in_piped_consumption
        self.total_piped_consumption = total_hh_piped_consumption + total_co_piped_consumption + \
                                       total_rf_piped_consumption + total_in_piped_consumption

    def det_gov_metrics(self):
        pass

    def setup(self, timestamp):
        gov_codes = {"bal" : 12, "irb" : 21, "ajl" : 24, "jar" : 23, "maf" : 22, "zar" : 13, "amm" : 11, "mad" : 14,
                     "kar" : 31, "maa" : 33, "taf" : 32, "aqa" : 34}
        node_code = str(gov_codes[self.name[0:3]]) + "0101_urb"
        my_node = self.network.get_institution("all_urban_households").get_node(node_code)
        self.x = my_node.x
        self.y = my_node.y

        year = self.network.current_timestep.year

        self.update_tariff_structure(year)
        self.update_nrw_shares(year)


class SubdistAgentWrapper(JordanInstitution):
    """A subdistrict-level human module institution class."""
    description = "Subdist Agent Wrapper"

    def __init__(self, name, **kwargs):
        super(SubdistAgentWrapper, self).__init__(name, **kwargs)
        self.name = name
        self.subdist_code = int(name[8:14])
        self.gov = 0
        self.x = 0
        self.y = 0
        self.agents = []
        self.agents_no_industry = []
        self.hh_agents = []
        self.co_agents = []
        self.rf_agents = []
        self.in_agents = []

    _properties = {
        'max_tanker_distance': 0,
        'tanker_distances': [],
        'tanker_price': 0,
    }


class TankerMarket(JordanInstitution):
    """A Jordan-wide tanker water market institution class."""
    description = "Jordan-Wide Tanker Market Institution"

    def __init__(self, name, **kwargs):
        super(TankerMarket, self).__init__(name, **kwargs)
        self.transport_cost_coef = None
        self.is_tanker_market_first_run = True
        self.fwm = None
        self.opt = None
        self.result_xs = None
        self.minimum_farm_offer_constraint = None
        self.road_dist = None
        
    _properties = {
        'termination_condition': None,
        'solver_status': None
    }

    def reset_agent_variables(self, all_farms, all_subdists, all_consumers):
        for f in all_farms:
            f.gw_sales_to_tanker = []
            f.gw_sale_to_tanker = 0.0
            f.final_tanker_prices = []
            f.final_tanker_price = 0.0
            f.tanker_market_revenue = 0.0
        for d in all_subdists:
            d.tanker_price = 0.0
            d.max_tanker_distance = 0.0
            d.tanker_distances = []
        for a in all_consumers:
            a.tanker_consumption = 0.0
            if a.piped_consumption > 0.0:
                a.WTP_for_piped_consumption = a.get_marginal_willingness_to_pay()
            else:
                a.WTP_for_piped_consumption = 0.0
            a.final_tanker_price = 0.0
            a.final_tanker_price_at_farm = 0.0
            a.tanker_expenditure
            a.tanker_market_contract_list = {}
            a.tanker_distances = []
            a.tanker_sales_per_farm = []

    def get_tanker_market_distances(self):
        distance_file_name = self.network.parameters.tanker['filename_road_distances']
        distance_data_sheet = 'EuclDistXCircuityFactor'
        if self.network.simulation_type == "tanker":
            if self.network.parameters.tanker['tanker_market_extension'] == 1:
                distance_data_sheet = 'RoadDistances'
        self.road_dist = get_excel_data(distance_file_name).parse(distance_data_sheet)

    def det_tanker_market_contracts(self):
        ##########################################
        # 0. Resetting all agent variables, etc. #
        ##########################################

        ###########################################
        # 0.0. Basic parameters and input values: #
        max_tanker_sale_distance = self.network.parameters.tanker['max_tanker_sale_distance']
        if self.network.current_timestep_idx == 0:
            self.transport_cost_coef = self.network.exogenous_inputs.tanker_market_global_params[
                "tanker_transport_cost_coefficient"][0]
            self.transport_cost_coef *= self.network.parameters.tanker['transport_cost_factor']
            self.minimum_farm_offer_constraint = self.network.parameters.tanker['minimum_farm_offer_constraint']

        ##########################################
        # 0.1. Full institution and agent lists: #
        gov_wrappers = list(self.network.get_institution('human_agent_wrapper').govs)
        all_farms = list(self.network.get_institution('all_highland_farms').nodes)
        all_consumers = list(self.network.get_institution('human_agent_wrapper').agents_no_industry)
        all_subdists = list(self.network.get_institution('human_agent_wrapper').subdists)

        #######################################
        # 0.2. Resetting all agent variables: #
        self.reset_agent_variables(all_farms, all_subdists, all_consumers)

        #########################################
        # 1. Preparation of non-Pyomo variables #
        #########################################

        ########################################################
        # 1.2. Tanker-market-only institution and agent lists: #
        all_fewer_farms = [[f] for f in all_farms if f.is_active_in_tanker_market]
        if self.is_tanker_market_first_run:
            self.get_tanker_market_distances()

        ###################################################################################
        # 1.3. Subdistricts', households, and more farms' non-Pyomo variable preparation: #
        # 1.3.1. Indices
        hindices = range(len(all_consumers))
        hhindices = []
        hcindices = []
        findices = range(len(all_fewer_farms))
        fs_hindices = dict((f, []) for f in findices)
        hs_findices = dict((h, []) for h in hindices)
        # 1.3.2. Indices used only during setup
        dindices = range(len(all_subdists))
        ds_hindices = dict((d, []) for d in dindices)
        hs_dindex = {}
        ds_findices = {}
        fs_dindices = dict((f, []) for f in findices)
        # 1.3.3. Parameters
        repres_units = {}
        cs_coefs = {}
        cs_expos = {}
        pw_consps = {}
        pw_csurpls = {}
        min_prices = {}
        max_offer_constraints = {}
        is_hix_actives = dict((h, float("inf")) for h in hindices)
        # 1.3.4. Variables
        x_start_values = {}

        # 1.3.5. Main assignment loop
        for h in hindices:
            ha = all_consumers[h]
            hs_dindex[h] = all_subdists.index(ha.subdist)
            ds_hindices[hs_dindex[h]].append(h)
            if ha.type != "commercial":
                hhindices+=[h]
            else:
                hcindices+=[h]
            if (ha.represented_units > 0.0) and (ha.piped_consumption > 0.0):
                repres_units[h] = ha.represented_units
                cs_coefs[h] = ha.sigma
                cs_expos[h] = ha.sigma2
                pw_consps[h] = ha.piped_consumption
                if ha.type != "commercial":
                    pw_csurpls[h] = (((ha.sigma * log(ha.piped_consumption)) - ha.sigma + ha.sigma2) *
                                     ha.piped_consumption)
                else:
                    pw_csurpls[h] = (((ha.sigma * log(ha.piped_consumption)) - ha.sigma + ha.sigma2) *
                                     ha.piped_consumption)
            else:
                repres_units[h] = 1.0
                cs_coefs[h] = 0.0
                cs_expos[h] = 0.0
                pw_consps[h] = 0.0
                pw_csurpls[h] = 0.0
                is_hix_actives[h] = 0.0

        for d in dindices:
            da = all_subdists[d]
            curr_findices = []
            for f in findices:
                fas = all_fewer_farms[f]
                fas_pos = list(f for f in fas if (f.tanker_offer_quantity > 0.0))
                fas_no = len(fas_pos)
                fa0 = fas[0]
                new_road_distance = float(self.road_dist.loc[(self.road_dist["rows-towns_cols-wells"] ==
                                                               da.subdist_code), int(fa0.subdistrict)]) / 1000.
                if (new_road_distance <= max_tanker_sale_distance) or (int(fa0.subdistrict) == da.subdist_code):
                    curr_findices.append(f)
                    fs_dindices[f].append(d)
                    if fas_no > 0:
                        offer_sum = sum(fa.tanker_offer_quantity for fa in fas_pos)
                        max_offer_constraints[f] = offer_sum / (365.0 / 12.0)
                        mean_price = sum(fa.tanker_water_price for fa in fas_pos) / fas_no
                    else:
                        max_offer_constraints[f] = self.minimum_farm_offer_constraint
                        mean_price = 1.0
                    for h in ds_hindices[d]:
                        x_start_values[h, f] = 0.0
                        fs_hindices[f].append(h)
                        hs_findices[h].append(f)
                        min_prices[h, f] = (self.transport_cost_coef * new_road_distance) + mean_price

            ds_findices[d] = curr_findices

        #############################################
        # 2. FORK: Initial model run or warm start: #
        #############################################
        if self.is_tanker_market_first_run:
            self.is_tanker_market_first_run = False

            #######################################################
            # A.2. Pyomo model creation and component definition: #
            #######################################################
            # A.2.1. Model creation:
            fwm = ConcreteModel()  # AbstractModel()
            # A.2.2. Index definition:
            fwm.hixs = Set(initialize=hindices)
            fwm.fixs = Set(initialize=findices)
            fwm.fs_hixs = Set(fwm.fixs, initialize=fs_hindices)
            fwm.hs_fixs = Set(fwm.hixs, initialize=hs_findices)
            # A.2.3. Parameter definition:
            fwm.hunits = Param(fwm.hixs, initialize=repres_units, mutable=True)
            fwm.coefs = Param(fwm.hixs, initialize=cs_coefs, mutable=True)
            fwm.expos = Param(fwm.hixs, initialize=cs_expos, mutable=True)
            fwm.pw_qs = Param(fwm.hixs, initialize=pw_consps, mutable=True)
            fwm.pw_css = Param(fwm.hixs, initialize=pw_csurpls, mutable=True)
            fwm.min_prcs = Param(fwm.hixs, fwm.fixs, initialize=min_prices, mutable=True)
            fwm.f_qmax = Param(fwm.fixs, initialize=max_offer_constraints, mutable=True)
            fwm.is_hix_actives = Param(fwm.hixs, initialize=is_hix_actives, mutable=True)
            # A.2.4. Variable definition:
            fwm.xs = Var(fwm.hixs, fwm.fixs, domain=NonNegativeReals, initialize=x_start_values)

            fwm.hixs2 = Set(initialize=hindices)
            fwm.hhixs2 = Set(initialize=hhindices)
            fwm.hcixs2 = Set(initialize=hcindices)
            fwm.fixs2 = Set(initialize=findices)
            fwm.fs_hixs2 = Set(fwm.fixs2, initialize=fs_hindices)
            fwm.hs_fixs2 = Set(fwm.hixs2, initialize=hs_findices)

            def obj_fun(fwm):
                return (sum(((
                    (
                        ((fwm.coefs[h] *
                          log((sum(fwm.xs[h, f] for f in fwm.hs_fixs2[h]) / fwm.hunits[h]) + fwm.pw_qs[h])) -
                         fwm.coefs[h] + fwm.expos[h]) *
                        ((sum(fwm.xs[h, f] for f in fwm.hs_fixs2[h]) / fwm.hunits[h]) + fwm.pw_qs[h])
                    ) - fwm.pw_css[h] -
                    (sum((fwm.min_prcs[h, f] * fwm.xs[h, f]) for f in fwm.hs_fixs2[h]) / fwm.hunits[h])
                            ) * fwm.hunits[h]) for h in fwm.hcixs2) +
                    sum(((
                    (
                        ((fwm.coefs[h] *
                          log((sum(fwm.xs[h, f] for f in fwm.hs_fixs2[h]) / fwm.hunits[h]) + fwm.pw_qs[h])) -
                         fwm.coefs[h] + fwm.expos[h]) *
                        ((sum(fwm.xs[h, f] for f in fwm.hs_fixs2[h]) / fwm.hunits[h]) + fwm.pw_qs[h])
                    ) - fwm.pw_css[h] -
                    (sum((fwm.min_prcs[h, f] * fwm.xs[h, f]) for f in fwm.hs_fixs2[h]) / fwm.hunits[h])
                         ) * fwm.hunits[h]) for h in fwm.hhixs2))
            fwm.obj_f = Objective(rule=obj_fun, sense=maximize)

            def sales_ceiling_constraint(fwm, f):
                return sum(fwm.xs[h, f] for h in fwm.fs_hixs2[f]) <= fwm.f_qmax[f]
            fwm.c3 = Constraint(fwm.fixs2, rule=sales_ceiling_constraint)

            def inactivity_constraint(fwm, h):
                return sum(fwm.xs[h, f] for f in fwm.hs_fixs2[h]) <= fwm.is_hix_actives[h]
            fwm.c4 = Constraint(fwm.hixs2, rule=inactivity_constraint)

            #######################################
            # A.3. Tanker market policies 2 & 3:  #
            #######################################
            # A.3.1. Policy 2: Tanker license cap:
            if self.network.simulation_type == "tanker":
                if self.network.parameters.tanker['tanker_policy_selection'] == 2:
                    haw = self.network.get_institution("human_agent_wrapper")
                    ts_now1 = self.network.current_timestep_idx
                    if ts_now1 > 11:
                        past_years = self.network.current_timestep.year - 2015
                        this_month = self.network.current_timestep.month - 1
                        past_this_months = [((i * 12) + this_month) for i in range(past_years)]
                        past_all_months = [i for i in range(past_years * 12)]
                        sum_past_sales_avg_month = sum(
                            haw.get_history("total_tanker_consumption")[i] for i in past_all_months) / 12.
                        sum_past_sales_this_month = sum(
                            haw.get_history("total_tanker_consumption")[i] for i in past_this_months)
                        past_sales_share_avg_this_month = sum_past_sales_this_month / sum_past_sales_avg_month
                    else:
                        first_year_sales_avgs = \
                            self.network.exogenous_inputs.tanker_market_global_params.first_year_sales_avgs.values.tolist()
                        past_sales_share_avg_this_month = first_year_sales_avgs[ts_now1]
                    tanker_cap1 = past_sales_share_avg_this_month * np.linspace(58560000., 58560000., 36 * 12)[
                        self.network.current_timestep_idx] / 365.
                    fwm.tanker_cap1 = Param(initialize=tanker_cap1, mutable=True)

                    def total_tanker_sales_ceiling(fwm):
                        return sum(sum(fwm.xs[h, f] for f in fwm.hs_fixs2[h]) for h in fwm.hixs2) <= fwm.tanker_cap1
                    fwm.c5 = Constraint(rule=total_tanker_sales_ceiling)
                    print("Tanker water market analyses - tanker license cap: " + str(tanker_cap1))

            # A.3.2. Policy 3: Household priority cap:
            if self.network.simulation_type == "tanker":
                if self.network.parameters.tanker['tanker_policy_selection'] == 3:
                    haw = self.network.get_institution("human_agent_wrapper")
                    ts_now1 = self.network.current_timestep_idx
                    if ts_now1 > 11:
                        past_years = self.network.current_timestep.year - 2015
                        this_month = self.network.current_timestep.month - 1
                        past_this_months = [((i * 12) + this_month) for i in range(past_years)]
                        past_all_months = [i for i in range(past_years * 12)]
                        sum_past_sales_avg_month = sum(
                            haw.get_history("total_tanker_consumption")[i] for i in past_all_months) / 12.
                        sum_past_sales_this_month = sum(
                            haw.get_history("total_tanker_consumption")[i] for i in past_this_months)
                        past_sales_share_avg_this_month = sum_past_sales_this_month / sum_past_sales_avg_month
                    else:
                        first_year_sales_avgs = \
                            self.network.exogenous_inputs.tanker_market_global_params.first_year_sales_avgs.values.tolist()
                        past_sales_share_avg_this_month = first_year_sales_avgs[ts_now1]
                    tanker_cap1 = past_sales_share_avg_this_month * np.linspace(58560000., 58560000., 36 * 12)[
                        self.network.current_timestep_idx] / 365.
                    cap_year1 = self.network.current_timestep.year - 2015
                    cap_month1 = self.network.current_timestep.month - 1
                    pop_factor1 = \
                        self.network.exogenous_inputs.tanker_market_global_params.population_factor.values.tolist()[cap_year1]
                    hh_share_by_month1 = \
                        self.network.exogenous_inputs.tanker_market_global_params.hhld_share_by_month.values.tolist()[cap_month1]
                    hh_tanker_cap1 = pop_factor1 * hh_share_by_month1 * tanker_cap1
                    co_tanker_cap1 = max(0.0, (tanker_cap1 - hh_tanker_cap1))
                    fwm.hh_tanker_cap1 = Param(initialize=hh_tanker_cap1, mutable=True)
                    fwm.co_tanker_cap1 = Param(initialize=co_tanker_cap1, mutable=True)

                    def hh_tanker_sales_ceiling(fwm):
                        return sum(sum(fwm.xs[h, f] for f in fwm.hs_fixs2[h]) for h in fwm.hhixs2) <= fwm.hh_tanker_cap1
                    fwm.c5 = Constraint(rule=hh_tanker_sales_ceiling)

                    def co_tanker_sales_ceiling(fwm):
                        return sum(sum(fwm.xs[h, f] for f in fwm.hs_fixs2[h]) for h in fwm.hcixs2) <= fwm.co_tanker_cap1
                    fwm.c6 = Constraint(rule=co_tanker_sales_ceiling)

                    print("Tanker water market analyses - household priority cap:")
                    print(" - Overall tanker sales cap:   " + str(tanker_cap1))
                    print(" - Cap on sales to households: " + str(hh_tanker_cap1))
                    print(" - Cap on sales to businesses: " + str(co_tanker_cap1))

            #################################
            # A.4. Running the Pyomo model: #
            #################################

            # A.4.1. Warm start: First get & set up solver: #
            self.opt = SolverFactory("ipopt", solver_io='nl')

            # A.4.2 Warm start: Preparing & saving inputs: #
            fwm.ipopt_zL_out = Suffix(direction=Suffix.IMPORT)
            fwm.ipopt_zU_out = Suffix(direction=Suffix.IMPORT)
            fwm.ipopt_zL_in = Suffix(direction=Suffix.EXPORT)
            fwm.ipopt_zU_in = Suffix(direction=Suffix.EXPORT)
            fwm.dual = Suffix(direction=Suffix.IMPORT_EXPORT)

            # A.4.3 Calculating the initial solution: #
            self.solver_status = "pre-run"
            try:
                results = self.opt.solve(fwm, keepfiles=False)
                self.termination_condition = results.solver.termination_condition
                self.solver_status = "ok"
            except (ValueError, pyutilib.common._exceptions.ApplicationError) as error:
                self.solver_status = "ipopt failed once"
                logging.info("ipopt did not converge ... retrying")
                try:
                    results = self.opt.solve(fwm, keepfiles=False)
                    self.termination_condition = results.solver.termination_condition
                    self.solver_status = "ok"
                except (ValueError, pyutilib.common._exceptions.ApplicationError) as error:
                    self.solver_status = "ipopt failed twice"
                    logging.info("ipopt did not converge")

            # A.4.4 Storing the model: #
            self.fwm = fwm

        else:
            #####################################
            # B.2. Prepare warmed-up model run: #
            #####################################
            fwm = self.fwm

            ##########################################
            # B.3. Preparing the warmed-up solution: #
            ##########################################
            fwm.ipopt_zL_in.update(fwm.ipopt_zL_out)
            fwm.ipopt_zU_in.update(fwm.ipopt_zU_out)
            self.opt.options['warm_start_init_point'] = 'yes'
            self.opt.options['warm_start_bound_push'] = 1e-6
            self.opt.options['warm_start_mult_bound_push'] = 1e-6
            self.opt.options['mu_init'] = 1e-6

            for f in findices:
                fwm.f_qmax[f] = max_offer_constraints[f]

            for h in hindices:
                fwm.hunits[h] = repres_units[h]
                fwm.coefs[h] = cs_coefs[h]
                fwm.expos[h] = cs_expos[h]
                fwm.pw_qs[h] = pw_consps[h]
                fwm.pw_css[h] = pw_csurpls[h]
                fwm.is_hix_actives[h] = is_hix_actives[h]
                for f in hs_findices[h]:
                    fwm.min_prcs[h, f] = min_prices[h, f]

            ######################################
            # B.4. Tanker market policies 2 & 3: #
            ######################################
            # B.4.1. Policy 2: Tanker license cap:
            if self.network.simulation_type == "tanker":
                if self.network.parameters.tanker['tanker_policy_selection'] == 2:
                    haw = self.network.get_institution("human_agent_wrapper")
                    ts_now1 = self.network.current_timestep_idx
                    if ts_now1 > 11:
                        past_years = self.network.current_timestep.year - 2015
                        this_month = self.network.current_timestep.month - 1
                        past_this_months = [((i * 12) + this_month) for i in range(past_years)]
                        past_all_months = [i for i in range(past_years * 12)]
                        # sum_past_sales_avg_month = sum(haw.get_history("total_tanker_consumption")) / 12.
                        sum_past_sales_avg_month = sum(
                            haw.get_history("total_tanker_consumption")[i] for i in past_all_months) / 12.
                        sum_past_sales_this_month = sum(
                            haw.get_history("total_tanker_consumption")[i] for i in past_this_months)
                        past_sales_share_avg_this_month = sum_past_sales_this_month / sum_past_sales_avg_month
                    else:
                        first_year_sales_avgs = \
                            self.network.exogenous_inputs.tanker_market_global_params.first_year_sales_avgs.values.tolist()
                        past_sales_share_avg_this_month = first_year_sales_avgs[ts_now1]
                    tanker_cap1 = past_sales_share_avg_this_month * np.linspace(58560000., 58560000., 36 * 12)[
                        self.network.current_timestep_idx] / 365.
                    fwm.tanker_cap1 = tanker_cap1
                    print("Tanker water market analyses - tanker license cap: " + str(tanker_cap1))

            # B.4.2. Policy 3: Household priority cap:
            if self.network.simulation_type == "tanker":
                if self.network.parameters.tanker['tanker_policy_selection'] == 3:
                    haw = self.network.get_institution("human_agent_wrapper")
                    ts_now1 = self.network.current_timestep_idx
                    if ts_now1 > 11:
                        past_years = self.network.current_timestep.year - 2015
                        this_month = self.network.current_timestep.month - 1
                        past_this_months = [((i * 12) + this_month) for i in range(past_years)]
                        past_all_months = [i for i in range(past_years * 12)]
                        sum_past_sales_avg_month = sum(
                            haw.get_history("total_tanker_consumption")[i] for i in past_all_months) / 12.
                        sum_past_sales_this_month = sum(
                            haw.get_history("total_tanker_consumption")[i] for i in past_this_months)
                        past_sales_share_avg_this_month = sum_past_sales_this_month / sum_past_sales_avg_month
                    else:
                        first_year_sales_avgs = \
                            self.network.exogenous_inputs.tanker_market_global_params.first_year_sales_avgs.values.tolist()
                        past_sales_share_avg_this_month = first_year_sales_avgs[ts_now1]
                    tanker_cap1 = past_sales_share_avg_this_month * np.linspace(58560000., 58560000., 36 * 12)[
                        self.network.current_timestep_idx] / 365.
                    cap_year1 = self.network.current_timestep.year - 2015
                    cap_month1 = self.network.current_timestep.month - 1
                    pop_factor1 = \
                        self.network.exogenous_inputs.tanker_market_global_params.population_factor.values.tolist()[cap_year1]
                    hh_share_by_month1 = \
                        self.network.exogenous_inputs.tanker_market_global_params.hhld_share_by_month.values.tolist()[cap_month1]
                    hh_tanker_cap1 = pop_factor1 * hh_share_by_month1 * tanker_cap1
                    co_tanker_cap1 = max(0.0, (tanker_cap1 - hh_tanker_cap1))
                    fwm.hh_tanker_cap1 = hh_tanker_cap1
                    fwm.co_tanker_cap1 = co_tanker_cap1

                    print("Tanker water market analyses - household priority cap:")
                    print(" - Overall tanker sales cap:   " + str(tanker_cap1))
                    print(" - Cap on sales to households: " + str(hh_tanker_cap1))
                    print(" - Cap on sales to businesses: " + str(co_tanker_cap1))

            ###########################################
            # B.5 Calculating the warmed-up solution: #
            ###########################################
            self.solver_status = "pre-run"
            try:
                results = self.opt.solve(fwm, keepfiles=False)
                self.termination_condition = results.solver.termination_condition
                self.solver_status = "ok"
            except (ValueError, pyutilib.common._exceptions.ApplicationError) as error:
                self.solver_status = "ipopt failed once"
                logging.info("ipopt did not converge ... retrying")
                try:
                    results = self.opt.solve(fwm, keepfiles=False)
                    self.termination_condition = results.solver.termination_condition
                    self.solver_status = "ok"
                except (ValueError, pyutilib.common._exceptions.ApplicationError) as error:
                    self.solver_status = "ipopt failed twice"
                    logging.info("ipopt did not converge ... re-using previous results")

        ################################
        # 4. Updating agent variables: #
        ################################
        if self.solver_status == "ok":
            logging.info("Tanker water market solved using IPOPT")
            self.result_xs = dict(self.fwm.xs.get_values())
        else:
            logging.info("Tanker water market solver did not converge ... previous results loaded")

        for d in dindices:
            da = all_subdists[d]
            da.tanker_price = 0.0
            for f in ds_findices[d]:
                subdist_purchase = sum(self.result_xs[h, f] for h in ds_hindices[d])
                if subdist_purchase > 0.0:
                    fas = all_fewer_farms[f]
                    fas_no = len(fas)
                    fa0 = fas[0]
                    distance = float(self.road_dist.loc[(self.road_dist["rows-towns_cols-wells"] ==
                                                          da.subdist_code), int(fa0.subdistrict)]) / 1000.
                    da.tanker_distances += [distance]
                    if distance > da.max_tanker_distance:
                        da.max_tanker_distance = distance
                    mean_price = sum(fa.tanker_water_price for fa in fas) / fas_no
                    min_subdist_price = mean_price + (self.transport_cost_coef * distance)
                    if min_subdist_price > da.tanker_price:
                        da.tanker_price = min_subdist_price

            for f in ds_findices[d]:
                subdist_purchase = sum(self.result_xs[h, f] for h in ds_hindices[d])
                if subdist_purchase > 0.0:
                    fas = all_fewer_farms[f]
                    fa0 = fas[0]
                    distance = float(self.road_dist.loc[(self.road_dist["rows-towns_cols-wells"] ==
                                                          da.subdist_code), int(fa0.subdistrict)]) / 1000.
                    offer_sum = sum(fa.tanker_offer_quantity for fa in fas)
                    for fa in fas:
                        if offer_sum > 0.0:
                            sales_factor = fa.tanker_offer_quantity / offer_sum
                            _tanker_sale = subdist_purchase * sales_factor
                            fa.gw_sales_to_tanker.append(_tanker_sale)
                            fa.gw_sale_to_tanker += _tanker_sale
                            _tanker_price = da.tanker_price - (self.transport_cost_coef * distance)
                            fa.final_tanker_prices.append(_tanker_price)
                            fa.tanker_market_revenue += _tanker_price * _tanker_sale
                            if fa.gw_sale_to_tanker > 0.0:
                                fa.final_tanker_price = fa.tanker_market_revenue / fa.gw_sale_to_tanker

            for h in ds_hindices[d]:
                a = all_consumers[h]
                if a.represented_units > 0.0:
                    a.tanker_consumption = sum(self.result_xs[h, f] for f in ds_findices[d]) / a.represented_units
                    a.final_tanker_price = da.tanker_price
                    a.tanker_expenditure = a.final_tanker_price * a.tanker_consumption * a.represented_units
                    a.expenditure += a.tanker_expenditure
                    if (a.type == "commercial") and ((a.piped_consumption + a.tanker_consumption) > 0):
                        a.final_tanker_price_plus_half_WTP = da.tanker_price + max(0,
                            (0.5 * ((a.sigma * log(a.piped_consumption + a.tanker_consumption)) - a.sigma +
                                    a.sigma2) * (a.piped_consumption + a.tanker_consumption) - da.tanker_price))
                    elif (a.piped_consumption + a.tanker_consumption) > 0:
                        a.final_tanker_price_plus_half_WTP = da.tanker_price + max(0,
                            (0.5 * ((a.sigma * log(a.piped_consumption + a.tanker_consumption)) - a.sigma +
                                    a.sigma2) * (a.piped_consumption + a.tanker_consumption) - da.tanker_price))
                    weighted_distance_sum = 0
                    for f in ds_findices[d]:
                        if self.result_xs[h, f] > 0:
                            fa0 = all_fewer_farms[f][0]
                            distance = float(self.road_dist.loc[(self.road_dist["rows-towns_cols-wells"] ==
                                                                  da.subdist_code), int(fa0.subdistrict)]) / 1000.
                            weighted_distance_sum += distance * self.result_xs[h, f]
                            a.tanker_distances+=[distance]
                            a.tanker_sales_per_farm+=[(self.result_xs[h, f] / a.represented_units)]
                    if (a.tanker_consumption * a.represented_units) > 0:
                        a.tanker_distance_avg = weighted_distance_sum / (a.tanker_consumption * a.represented_units)
                    else:
                        a.tanker_distance_avg = 0
                else:
                    a.tanker_consumption = 0
                    a.final_tanker_price = 0
                    a.tanker_expenditure = 0
                    a.final_tanker_price_plus_half_WTP = 0
                    a.tanker_distance_avg = 0

        for g in gov_wrappers:
            g.det_tanker_water_consumption()
        self.network.get_institution('human_agent_wrapper').det_tanker_water_consumption()
