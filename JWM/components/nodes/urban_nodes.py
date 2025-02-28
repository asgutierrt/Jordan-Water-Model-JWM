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

from JWM.components.nodes.network_nodes import JordanNode
from pynsim.components.component import Component
from math import log, exp
from scipy import optimize
from JWM import get_pickle

baseline_consumption_pickle = get_pickle("baseline_consumption.p")
baseline_expenditure_pickle = get_pickle("baseline_expenditure.p")
baseline_represented_units_pickle = get_pickle("baseline_represented_units.p")


class UrbanHHAgent(JordanNode):
    """ Node representation of the urban household agent class. """
    description = "A node representing urban households in a sub-district"

    colour = "white"
    shape  = "^"
    size   = "small"

    def __init__(self, name, **kwargs):
        super(UrbanHHAgent, self).__init__(name, **kwargs)
        self.name = name

    _properties = {
        'population': 0,
        }

    def setup(self, timestep):
        self.population = self.network.exogenous_inputs.population[int(self.name[0:6])]


class HumanHHAgent(Component):
    """ A household agent class, at the subdistrict level. This is a basic PyNSim component, not a node. """
    description = "An household agent class, at the subdistrict level. Not a node."

    def __init__(self, name, gov, gov_name, subdist_number, subdist_code, subdist_no, subdist_name, x, y,
                 represented_units, coefs, params, duration, duration_pop_weight, storage, month_factors, tanker_prices,
                 is_sewage, sewage_pop_weight, **kwargs):
        super(HumanHHAgent, self).__init__(name, **kwargs)

        # 1. Parameters:
        self.name = name
        self.type = "household"
        self.gov = gov
        self.gov_name = gov_name
        self.subdist_number = subdist_number
        self.subdist_code = subdist_code
        self.subdist_no = subdist_no
        self.subdist_name = subdist_name
        self.governorate = None
        self.subdist = None
        self.x = x
        self.y = y
        self.initial_represented_units = represented_units
        self.represented_units = represented_units
        self.coefs = coefs
        self.params = params
        self.duration_base = (duration / 24.0)
        self.duration = self.duration_base
        self.duration_pop_weight = duration_pop_weight
        self.storage_base = storage
        self.storage = self.storage_base

        self.month_factors_seasonality = list(month_factors)
        self.month_factors_base = list(month_factors)
        self.month_factors = list(month_factors)

        self.tanker_prices = tanker_prices
        self.is_sewage = is_sewage
        self.sewage_pop_weight = sewage_pop_weight
        self.hh_df_factor = 1
        self.tariff_blocks = [[], [], [], []]

        # 2. Calculating derived parameter values:
        self.update_coefs_and_params(1)
        self.hnum = self.params['hnum']

        # 3. Variables which are not properties:
        self.final_tanker_price_at_farm = 0
        self.tanker_market_contract_list = {}
        self.tanker_distance = []
        self.tariff_block = 0
        self.tariff = 0
        self.tariff_fixed_cost = 0
        self.rsp = 0
        self.is_non_supply_day_demand_satisfied = False
        self.piped_non_supply_day_demand = 0

    _properties = {
        'piped_demand': 0,
        'piped_consumption': 0,
        'piped_non_supply_day_consumption': 0,
        'piped_supply_day_consumption': 0,
        'tanker_consumption': 0,
        'WTP_for_piped_consumption': 0,
        'final_tanker_price': 0,
        'final_tanker_price_plus_half_WTP': 0,
        'tanker_distances': [],
        'tanker_sales_per_farm': [],
        'tanker_distance_avg': 0,
        'total_consumption': 0,
        'piped_expenditure': 0,
        'tanker_expenditure': 0,
        'expenditure': 0,
        'consumer_surplus_diff': 0,
        'consumer_surplus_diff_per_unit': 0,

        'absolute_consumer_surplus': 0,
        'conseq_months_below_30_lcd': 0,
        'conseq_months_below_40_lcd': 0,
        'conseq_months_below_50_lcd': 0,
        'conseq_months_piped_below_30_lcd': 0,
        'conseq_months_piped_below_40_lcd': 0,
        'conseq_months_piped_below_50_lcd': 0,

        'storage_constraint': False,
        'piped_constraint': False,
        'represented_units': 0,
        'distance_average': 0,
        'lifeline_consumption': 0,
        'hnum': 0,
        'income': 0,  # CK201227: Income test

        'tanker_price_expectation': 0,
        'last_relevant_block_upper_bound': 1e9,
    }

    def update_params_historical(self):
        month = self.network.current_timestep.month
        year = self.network.current_timestep.year

        if (year > 2015) or ((year == 2015) and (month > 10)):
            year = 2015
            month = 10

        # 1. Load updated parameters:
        year_subdist_row = ((year - 2006) * 89) + self.subdist_number
        hh_subdistrict_params = self.network.exogenous_inputs.hh_subdistrict_params.ix[year_subdist_row, :]
        self.params = hh_subdistrict_params["constant":]
        self.hnum = self.params['hnum']

        # 2. Update populations:
        self.represented_units = self.network.exogenous_inputs.monthly_pop_etc_hist.loc[
            (self.network.exogenous_inputs.monthly_pop_etc_hist["year"]==year)&
            (self.network.exogenous_inputs.monthly_pop_etc_hist["month"]==month)&
            (self.network.exogenous_inputs.monthly_pop_etc_hist["subdist_code"] == self.subdist_code),
            "jordanian"
        ].values[0]
        self.represented_units += self.network.exogenous_inputs.monthly_pop_etc_hist.loc[
            (self.network.exogenous_inputs.monthly_pop_etc_hist["year"]==year)&
            (self.network.exogenous_inputs.monthly_pop_etc_hist["month"]==month)&
            (self.network.exogenous_inputs.monthly_pop_etc_hist["subdist_code"] == self.subdist_code),
            "non_jordanian_non_refugee"
        ].values[0]
        self.represented_units += self.network.exogenous_inputs.monthly_pop_etc_hist.loc[
            (self.network.exogenous_inputs.monthly_pop_etc_hist["year"]==year)&
            (self.network.exogenous_inputs.monthly_pop_etc_hist["month"]==month)&
            (self.network.exogenous_inputs.monthly_pop_etc_hist["subdist_code"] == self.subdist_code),
            "tourists"
        ].values[0]
        self.represented_units *= self.duration_pop_weight * self.sewage_pop_weight / self.hnum
        self.represented_units *= self.network.parameters.hh['population_factor']

        # 3. Update supply duration and storage:
        self.update_duration_storage_and_df_factors(month, year)

        # 4. Update parameters:
        self.params.loc["income"] = self.network.exogenous_inputs.monthly_pop_etc_hist.loc[
            (self.network.exogenous_inputs.monthly_pop_etc_hist["year"] == year) &
            (self.network.exogenous_inputs.monthly_pop_etc_hist["month"] == month) &
            (self.network.exogenous_inputs.monthly_pop_etc_hist["subdist_code"] == self.subdist_code),
            "income"
        ].values[0]
        self.params.loc["income"] *= self.network.parameters.hh['income_factor']
        self.update_coefs_and_params(month)

        # CK201227: Income test:
        self.income = self.params['income']

    def update_params_projection(self):
        month = self.network.current_timestep.month
        year = self.network.current_timestep.year

        # 1. Update populations:
        self.represented_units = self.network.exogenous_inputs.monthly_pop_etc_ssp.loc[
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["year"]==year)&
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["month"]==month)&
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["subdist_code"] == self.subdist_code),
            "jordanian"
        ].values[0]
        self.represented_units += self.network.exogenous_inputs.monthly_pop_etc_ssp.loc[
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["year"]==year)&
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["month"]==month)&
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["subdist_code"] == self.subdist_code),
            "non_jordanian_non_refugee"
        ].values[0]
        year_tourists = year
        if year >= 2100:
            year_tourists = 2099
        self.represented_units += self.network.exogenous_inputs.monthly_pop_etc_ssp.loc[
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["year"]==year_tourists)&
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["month"]==month)&
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["subdist_code"] == self.subdist_code),
            "tourists"
        ].values[0]
        self.represented_units *= self.duration_pop_weight * self.sewage_pop_weight / self.hnum
        self.represented_units *= self.network.parameters.hh['population_factor']

        # 2. Update supply duration and storage:
        self.update_duration_storage_and_df_factors(month, year)

        # 3. Update parameters: (NOTES: "update_coefs_and_params" needs to be called after income has its final value!)
        self.params.loc["income"] = self.network.exogenous_inputs.monthly_pop_etc_ssp.loc[
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["year"] == year) &
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["month"] == month) &
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["subdist_code"] == self.subdist_code),
            "income"
        ].values[0]
        self.params.loc["income"] *= self.network.parameters.hh['income_factor']
        self.update_coefs_and_params(month)

        self.income = self.params['income']

    def update_coefs_and_params(self, month):
        self.sum_of_constant_terms = sum([a * b for a, b in zip(self.coefs[0:13], self.params[0:13])])
        self.z2 = ((self.coefs[14] * log(self.params[14])) + self.sum_of_constant_terms)
        self.sigma = (1.0 / self.coefs[13])

        if hasattr(self, "network"):
            admin_nrw_corr_base = self.get_admin_nrw_share_after_tech_nrw_base()
            admin_nrw_corr = self.get_admin_nrw_share_after_tech_nrw()
            self.month_factors_base = [(i / (1.0 - admin_nrw_corr_base)) for i in self.month_factors_seasonality]
            self.month_factors = [(i / (1.0 - admin_nrw_corr)) for i in self.month_factors_seasonality]

        self.days_div_month_factor = 1.0 / self.month_factors_base[month - 1]
        self.sigma2 = (log(self.days_div_month_factor) - self.z2) / self.coefs[13]

    def update_duration_storage_and_df_factors(self, month, year):
        use_supply_duration_factors = self.network.parameters.human['use_supply_duration_factors']
        if use_supply_duration_factors == 1:
            supply_duration_factor = self.network.parameters.supply_duration_factors.loc[
                (self.network.parameters.supply_duration_factors["year"] == year) &
                (self.network.parameters.supply_duration_factors["month"] == month) &
                (self.network.parameters.supply_duration_factors["subdist_code"] == self.subdist_code),
                "supply_duration_factors"
            ].values[0]
            self.duration = self.duration_base * supply_duration_factor
        if self.duration > 7.0:
            self.duration = 7.0
        if self.network.parameters.misc['supply_hours_equal'] == 'yes':
            self.duration = (self.params['hnum'] / 10.01125004) * 7.0

        ########################################################
        # Tanker analysis investment 2: Equitable distribution #
        ########################################################
        if self.network.simulation_type == "tanker":
            if ((self.network.parameters.tanker['tanker_analysis_investment_2'] == 1) and
                    (self.network.parameters.misc['supply_hours_equal'] == 'yes')):
                self.duration = 7.0

        use_storage_factors = self.network.parameters.hh['use_household_storage_factors']
        if use_storage_factors == 1:
            storage_factor = self.network.parameters.storage_factors.loc[
                (self.network.parameters.storage_factors["year"] == year) &
                (self.network.parameters.storage_factors["month"] == month) &
                (self.network.parameters.storage_factors["subdist_code"] == self.subdist_code),
                "household_storage_factors"
            ].values[0]
            self.storage = self.storage_base * storage_factor
        if self.network.current_timestep_idx == 0:
            self.hh_df_factor = self.network.parameters.hh['demand_factor']

    def get_governorate(self):
        if self.governorate is None:
            govs = [i for i in self.network.institutions if i.component_type=='GovAgentWrapper']
            for g in govs:
                if g.gov_id == self.gov:
                    self.governorate = g
                    break
        return self.governorate

    def get_tariff_blocks(self):
        self.get_governorate()
        if self.is_sewage is True:
            return self.governorate.tariff_blocks['sewage']
        else:
            return self.governorate.tariff_blocks['nonsewage']

    def get_admin_nrw_share_after_tech_nrw(self):
        self.get_governorate()
        return self.governorate.admin_nrw_share_after_tech_nrw

    def get_admin_nrw_share_after_tech_nrw_base(self):
        self.get_governorate()
        return self.governorate.admin_nrw_share_after_tech_nrw_base

    def det_general_water_demand(self, price, linear_price_coef, month):
        if self.represented_units > 0:
            if linear_price_coef > 0:
                demand = (self.month_factors[month - 1] *
                          optimize.brentq(self.det_general_water_demand_helper, 1e-9, 10000,
                                          args=(price, linear_price_coef)))
            else:
                demand = (self.month_factors[month - 1] *
                          exp((self.coefs[13] * price) + (self.coefs[14] * log(self.params[14])) +
                           self.sum_of_constant_terms))
        else:
            demand = 0.0
        return self.hh_df_factor * demand

    def det_general_water_demand_helper(self, quantity, price, linear_price_coef):
        return (exp((self.coefs[13] * (price + (linear_price_coef * quantity))) +
                    (self.coefs[14] * log(self.params[14])) + self.sum_of_constant_terms) - quantity)

    def det_piped_water_demand(self, month):
        # 1. Determine last relevant piped water block based on the tanker price expectation (type):
        tanker_price_expectation_type = self.network.parameters.tanker['tanker_price_expectation_type']
        tanker_price_expectation = self.tanker_prices[month - 1]
        if tanker_price_expectation_type == 1:
            timestep = self.network.current_timestep_idx + 1
            if (timestep <= 13) or (self.get_history("final_tanker_price")[-12] <= 0):
                tanker_price_expectation = self.tanker_prices[month - 1]
            else:
                tanker_price_expectation = (self.get_history("tanker_price_expectation")[-12] +
                                            self.get_history("final_tanker_price")[-12]) / 2.0
        if tanker_price_expectation_type == 2:
            tanker_price_expectation = 1e9
        self.tanker_price_expectation = tanker_price_expectation
        tariff_blocks = self.get_tariff_blocks()

        last_relevant_block = len(tariff_blocks[4])
        for i in tariff_blocks[4]:
            if tanker_price_expectation < i:
                last_relevant_block = max(0, (last_relevant_block - 1))

        self.last_relevant_block_upper_bound = tariff_blocks[0][last_relevant_block] / 91.25

        # 2. Set up key parameters: (NRW and storage)
        admin_nrw_corr = self.get_admin_nrw_share_after_tech_nrw()
        effective_storage = (self.storage / max(1e-9, (7.0 - self.duration)))

        # 3. Set initial demand value:
        if self.represented_units > 0:
            previous_demand = (tariff_blocks[0][1] / 91.25)
        else:
            previous_demand = 0.0

        # 3. Determine applicable tariff block and consumption quantity via loop:
        for block in range(1, last_relevant_block):
            # 3.1. Set up key block parameters:
            lower_bound = tariff_blocks[0][block] / 91.25
            upper_bound = tariff_blocks[0][block + 1] / 91.25
            current_tariff = tariff_blocks[1][block]
            linear_tariff_coef = tariff_blocks[2][block]
            prev_block_fixed_cost = tariff_blocks[3][block - 1] / 91.25
            current_fixed_cost = tariff_blocks[3][block] / 91.25

            # 3.2. Determine hypothetical demand for the current block: (accounting for fixed costs)
            hypothetical_demand = self.det_general_water_demand(current_tariff, linear_tariff_coef, month)
            demand_function_input_factor = (1.0 / self.month_factors[month - 1])
            block_bound_marginal_rate = max(0, (current_tariff + (linear_tariff_coef * demand_function_input_factor *
                                                                  lower_bound)))
            block_bound_avg_fixed_cost = max(0, ((current_fixed_cost - prev_block_fixed_cost) /
                                                 (hypothetical_demand - (lower_bound / (1.0 - admin_nrw_corr)))))
            fixed_cost_for_demand_function = block_bound_marginal_rate + (2.0 * block_bound_avg_fixed_cost)
            hypothetical_demand_with_fixed_cost = \
                self.det_general_water_demand(fixed_cost_for_demand_function, 0, month)
            billed_hypothetical_demand = (1 - admin_nrw_corr) * hypothetical_demand
            billed_hypothetical_demand_with_fixed_cost = (1 - admin_nrw_corr) * hypothetical_demand_with_fixed_cost

            # 3.3. Determine whether the agent would enter this block or stay in the previous one:
            if min(billed_hypothetical_demand, billed_hypothetical_demand_with_fixed_cost) > lower_bound:
                if billed_hypothetical_demand <= upper_bound:
                    previous_demand = hypothetical_demand
                else:
                    previous_demand = (upper_bound / (1.0 - admin_nrw_corr))

        # 4. Set unconstrained piped demand and storage-constrained piped demand for non-supply days:
        self.piped_demand = previous_demand
        self.piped_non_supply_day_demand = min(effective_storage, self.piped_demand)

    def det_consumer_surplus_diff(self):
        month = self.network.current_timestep.month
        total_consumption = (self.piped_consumption + self.tanker_consumption)
        baseline_total_consumption = float(baseline_consumption_pickle[self.name][month - 1])
        baseline_represented_units = float(baseline_represented_units_pickle[self.name][month - 1])

        self.consumer_surplus_diff_per_unit = \
            (0.0 if ((total_consumption <= 0.0) or (baseline_total_consumption <= 0.0)) else
             ((((self.sigma * log(total_consumption)) - self.sigma + self.sigma2) *
               total_consumption) -
              (((self.sigma * log(baseline_total_consumption)) - self.sigma + self.sigma2) *
               baseline_total_consumption) -
              (self.expenditure / self.represented_units) +
              (float(baseline_expenditure_pickle[self.name][month - 1]) / baseline_represented_units)))

        self.consumer_surplus_diff = self.consumer_surplus_diff_per_unit * self.represented_units

        self.absolute_consumer_surplus = (0.0 if (total_consumption <= 0.0) else
                                          ((((self.sigma * log(total_consumption)) - self.sigma + self.sigma2) *
                                            total_consumption * self.represented_units) - self.expenditure))

        if (total_consumption / self.hnum) < 0.03:
            if self.network.current_timestep_idx > 0:
                self.conseq_months_below_30_lcd = self.get_history("conseq_months_below_30_lcd")[-1] + 1
            else:
                self.conseq_months_below_30_lcd = 1
        else:
            self.conseq_months_below_30_lcd = 0

        if (total_consumption / self.hnum) < 0.04:
            if self.network.current_timestep_idx > 0:
                self.conseq_months_below_40_lcd = self.get_history("conseq_months_below_40_lcd")[-1] + 1
            else:
                self.conseq_months_below_40_lcd = 1
        else:
            self.conseq_months_below_40_lcd = 0

        if (total_consumption / self.hnum) < 0.05:
            if self.network.current_timestep_idx > 0:
                self.conseq_months_below_50_lcd = self.get_history("conseq_months_below_50_lcd")[-1] + 1
            else:
                self.conseq_months_below_50_lcd = 1
        else:
            self.conseq_months_below_50_lcd = 0

        if (self.piped_consumption / self.hnum) < 0.03:
            if self.network.current_timestep_idx > 0:
                self.conseq_months_piped_below_30_lcd = self.get_history("conseq_months_piped_below_30_lcd")[-1] + 1
            else:
                self.conseq_months_piped_below_30_lcd = 1
        else:
            self.conseq_months_piped_below_30_lcd = 0

        if (self.piped_consumption / self.hnum) < 0.04:
            if self.network.current_timestep_idx > 0:
                self.conseq_months_piped_below_40_lcd = self.get_history("conseq_months_piped_below_40_lcd")[-1] + 1
            else:
                self.conseq_months_piped_below_40_lcd = 1
        else:
            self.conseq_months_piped_below_40_lcd = 0

        if (self.piped_consumption / self.hnum) < 0.05:
            if self.network.current_timestep_idx > 0:
                self.conseq_months_piped_below_50_lcd = self.get_history("conseq_months_piped_below_50_lcd")[-1] + 1
            else:
                self.conseq_months_piped_below_50_lcd = 1
        else:
            self.conseq_months_piped_below_50_lcd = 0


    def get_marginal_willingness_to_pay(self):
        demand_function_quantity = (self.piped_consumption + self.tanker_consumption)
        return ((self.sigma * log(demand_function_quantity)) + self.sigma2)

    def setup(self, timestamp):
        if self.network.current_timestep.year <= 2015:
            self.update_params_historical()
        else:
            if self.network.current_timestep_idx == 0:
                self.update_params_historical()
            self.update_params_projection()


class HumanRFAgent(HumanHHAgent):
    """
    A refugee household agent class, at the subdistrict level, incl. refugee camps.
    This is a basic PyNSim component, not a node.
    """
    description = "A refugee household agent class, at the subdistrict level, incl. refugee camps. Not a node."

    def __init__(self, name, *args, **kwargs):
        super(HumanRFAgent, self).__init__(name, *args, **kwargs)
        self.type = "refugee"
        self.camp_groundwater_demand = 0
        self.is_camp_location = False

    _properties = {
        'piped_demand': 0,
        'piped_consumption': 0,
        'piped_non_supply_day_consumption': 0,
        'piped_supply_day_consumption': 0,
        'tanker_consumption': 0,
        'WTP_for_piped_consumption': 0,
        'final_tanker_price': 0,
        'final_tanker_price_plus_half_WTP': 0,
        'tanker_distances': [],
        'tanker_sales_per_farm': [],
        'tanker_distance_avg': 0,
        'total_consumption': 0,
        'piped_expenditure': 0,
        'tanker_expenditure': 0,
        'expenditure': 0,
        'consumer_surplus_diff': 0,
        'consumer_surplus_diff_per_unit': 0,

        'absolute_consumer_surplus': 0,
        'conseq_months_below_30_lcd': 0,
        'conseq_months_below_40_lcd': 0,
        'conseq_months_below_50_lcd': 0,
        'conseq_months_piped_below_30_lcd': 0,
        'conseq_months_piped_below_40_lcd': 0,
        'conseq_months_piped_below_50_lcd': 0,

        'storage_constraint': False,
        'piped_constraint': False,
        'represented_units': 0,
        'represented_units_syrian': 0,
        'distance_average': 0,
        'lifeline_consumption': 0,
        'hnum': 0,
        'income': 0,  # CK201227: Income test

        'tanker_price_expectation': 0,
        'last_relevant_block_upper_bound': 1e9,
    }

    def update_params_historical(self):
        month = self.network.current_timestep.month
        year = self.network.current_timestep.year

        if (year > 2015) or ((year == 2015) and (month > 10)):
            year = 2015
            month = 10

        # 1. Load updated parameters:
        year_subdist_row = ((year - 2006) * 89) + self.subdist_number
        rf_subdistrict_params = self.network.exogenous_inputs.rf_subdistrict_params.ix[year_subdist_row, :]
        self.params = rf_subdistrict_params["constant":]
        self.hnum = self.params['hnum']
        self.camp_groundwater_use = (rf_subdistrict_params["camp_water_use"] if self.is_camp_location else 0)

        # 2. Update populations:
        self.represented_units = self.network.exogenous_inputs.monthly_pop_etc_hist.loc[
            (self.network.exogenous_inputs.monthly_pop_etc_hist["year"]==year)&
            (self.network.exogenous_inputs.monthly_pop_etc_hist["month"]==month)&
            (self.network.exogenous_inputs.monthly_pop_etc_hist["subdist_code"] == self.subdist_code),
            "refugees_syrian"
        ].values[0]
        self.represented_units_syrian = self.represented_units
        self.represented_units += self.network.exogenous_inputs.monthly_pop_etc_hist.loc[
            (self.network.exogenous_inputs.monthly_pop_etc_hist["year"]==year)&
            (self.network.exogenous_inputs.monthly_pop_etc_hist["month"]==month)&
            (self.network.exogenous_inputs.monthly_pop_etc_hist["subdist_code"] == self.subdist_code),
            "refugees_other"
        ].values[0]
        self.represented_units *= self.duration_pop_weight * self.sewage_pop_weight / self.hnum
        self.represented_units_syrian *= self.duration_pop_weight * self.sewage_pop_weight / self.hnum
        if self.network.parameters.misc['refugee_counterfact'] == 'yes':
            self.represented_units -= self.represented_units_syrian
            self.represented_units_syrian = 0
        self.represented_units *= self.network.parameters.hh['population_factor']
        self.represented_units_syrian *= self.network.parameters.hh['population_factor']

        # 3. Applying remigration factor:
        self.update_and_apply_remigration_factor(month, year)

        # 4. Update supply duration and storage:
        self.update_duration_storage_and_df_factors(month, year)

        # 5. Update parameters: (NOTES: Updating income here needs to occur after loading the updated params in (1.)!;
        #                         "update_coefs_and_params" needs to be called after all values are finalized!)
        self.params.loc["income"] *= self.network.parameters.hh['income_factor']
        self.update_coefs_and_params(month)

        # CK201227: Income test:
        self.income = self.params['income']

    def update_params_projection(self):
        month = self.network.current_timestep.month
        year = self.network.current_timestep.year

        # 1. Load parameters:
        year_subdist_row = ((2015 - 2006) * 89) + self.subdist_number
        rf_subdistrict_params = self.network.exogenous_inputs.rf_subdistrict_params.ix[year_subdist_row, :]
        self.params = rf_subdistrict_params["constant":]

        # 2. Update populations:
        self.represented_units = self.network.exogenous_inputs.monthly_pop_etc_ssp.loc[
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["year"]==year)&
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["month"]==month)&
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["subdist_code"] == self.subdist_code),
            "refugees_syrian"
        ].values[0]
        self.represented_units_syrian = self.represented_units
        self.represented_units += self.network.exogenous_inputs.monthly_pop_etc_ssp.loc[
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["year"]==year)&
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["month"]==month)&
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["subdist_code"] == self.subdist_code),
            "refugees_other"
        ].values[0]
        self.represented_units *= self.duration_pop_weight * self.sewage_pop_weight / self.hnum
        self.represented_units_syrian *= self.duration_pop_weight * self.sewage_pop_weight / self.hnum
        if self.network.parameters.misc['refugee_counterfact'] == 'yes':
            self.represented_units -= self.represented_units_syrian
            self.represented_units_syrian = 0
        self.represented_units *= self.network.parameters.hh['population_factor']
        self.represented_units_syrian *= self.network.parameters.hh['population_factor']

        # 3. Applying remigration factor:
        self.update_and_apply_remigration_factor(month, year)

        # 4. Update supply duration and storage:
        self.update_duration_storage_and_df_factors(month, year)

        # 5. Update parameters:
        self.params.loc["income"] *= self.network.parameters.hh['income_factor']
        self.update_coefs_and_params(month)

        # CK201227: Income test:
        self.income = self.params['income']

    def update_and_apply_remigration_factor(self, month, year):
        refugee_remigration_type = self.network.parameters.rf['refugee_remigration_type']
        if refugee_remigration_type > 0:
            refugee_remigration_factor = self.network.parameters.remigration_factors.loc[
                (self.network.parameters.remigration_factors["year"] == year) &
                (self.network.parameters.remigration_factors["month"] == month) &
                (self.network.parameters.remigration_factors["subdist_code"] == self.subdist_code),
                ("remigration_factors_" + str(refugee_remigration_type))
            ].values[0]
            self.represented_units *= refugee_remigration_factor
            self.represented_units_syrian *= refugee_remigration_factor

    def det_well_water_demand(self):
        self.camp_groundwater_demand = self.camp_groundwater_use


class HumanCOAgent(Component):
    """A commercial agent class, at the subdistrict level. This is a basic PyNSim component, not a node."""
    description = "A commercial agent class, at the subdistrict level. Not a node."

    def __init__(self, name, gov, gov_name, size_class, subdist_code, subdist_no, subdist_name, x, y,
                 represented_units, coefs, params, duration, duration_pop_weight, connection_rate, sewage_rate,
                 summer_months, tanker_prices, connection_size, **kwargs):
        super(HumanCOAgent, self).__init__(name, **kwargs)
        # 1. Parameters:
        self.name = name
        self.type = "commercial"
        self.gov = gov
        self.gov_name = gov_name
        self.size_class = size_class
        self.subdist_code = subdist_code
        self.subdist_no = subdist_no
        self.subdist_name = subdist_name
        self.governorate = None
        self.subdist = None
        self.x = x
        self.y = y
        self.initial_represented_units = represented_units
        self.represented_units = represented_units
        self.coefs = coefs
        self.params = params
        self.duration_base = (duration / 24.0)
        self.duration = self.duration_base
        self.duration_pop_weight = duration_pop_weight
        self.storage_base = params[7]
        self.storage = self.storage_base
        self.connection_rate = connection_rate
        self.sewage_rate = sewage_rate
        self.summer_months = summer_months
        self.tanker_prices = tanker_prices
        self.connection_size = connection_size
        self.co_df_factor = 1
        self.tariff_blocks = [[0, 6, float("inf")], 0, [0, 0]]

        # 2. Calculating derived parameter values:
        self.sum_of_constant_terms = sum([a*b for a,b in zip(coefs[0:8],params[0:8])])
        price_coef_size_distinction = coefs[9]
        if self.size_class == 0:
            price_coef_size_distinction = coefs[11]
        self.coefs[9] = price_coef_size_distinction
        self.sum_of_constant_terms += self.coefs[10]*self.params[10]
        self.update_coefs_and_params(1)

        # 3. Variables which are not properties:
        self.final_tanker_price_at_farm = 0
        self.tanker_market_contract_list = {}
        self.tanker_distance = []
        self.tariff_block = 0
        self.tariff = 0
        self.tariff_fixed_cost = 0
        self.rsp = 0
        self.is_non_supply_day_demand_satisfied = False
        self.piped_non_supply_day_demand = 0

    _properties = {
        'piped_demand': 0,
        'piped_consumption': 0,
        'piped_non_supply_day_consumption': 0,
        'piped_supply_day_consumption': 0,
        'tanker_consumption': 0,
        'WTP_for_piped_consumption': 0,
        'final_tanker_price': 0,
        'final_tanker_price_plus_half_WTP': 0,
        'tanker_distances': [],
        'tanker_sales_per_farm': [],
        'tanker_distance_avg': 0,
        'total_consumption': 0,
        'piped_expenditure': 0,
        'tanker_expenditure': 0,
        'expenditure': 0,
        'consumer_surplus_diff': 0,
        'consumer_surplus_diff_per_unit': 0,

        'absolute_consumer_surplus': 0,

        'storage_constraint': False,
        'piped_constraint': False,
        'represented_units': 0,
        'distance_average': 0,
        'lifeline_consumption': 0,

        'tanker_price_expectation': 0,
        'last_relevant_block_upper_bound': 1e9,
    }

    def update_params_historical(self):
        month = self.network.current_timestep.month
        year = self.network.current_timestep.year

        if (year > 2015) or ((year == 2015) and (month > 10)):
            year = 2015
            month = 10

        # 1. Update populations:
        self.represented_units = self.network.exogenous_inputs.monthly_pop_etc_hist.loc[
            (self.network.exogenous_inputs.monthly_pop_etc_hist["year"]==year)&
            (self.network.exogenous_inputs.monthly_pop_etc_hist["month"]==month)&
            (self.network.exogenous_inputs.monthly_pop_etc_hist["subdist_code"] == self.subdist_code),
            ("establishments_size_" + str(self.size_class+1))
        ].values[0]
        self.represented_units *= self.duration_pop_weight
        self.represented_units *= self.network.parameters.co['population_factor']

        # 2. Update and apply tariff factor:
        self.update_and_apply_tariff_factor(year)

        # 3. Update supply duration, storage, and the demand function factor:
        self.update_duration_storage_and_df_factors(month, year)

        # 4. Update parameters:
        self.update_coefs_and_params(month)

    def update_params_projection(self):
        month = self.network.current_timestep.month
        year = self.network.current_timestep.year

        # 1. Update populations:
        self.represented_units = self.network.exogenous_inputs.monthly_pop_etc_ssp.loc[
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["year"]==year)&
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["month"]==month)&
            (self.network.exogenous_inputs.monthly_pop_etc_ssp["subdist_code"] == self.subdist_code),
            ("establishments_size_" + str(self.size_class+1))
        ].values[0]
        self.represented_units *= self.duration_pop_weight
        self.represented_units *= self.network.parameters.co['population_factor']

        # 2. Update and apply tariff factor:
        self.update_and_apply_tariff_factor(year)

        # 3. Update supply duration, storage, and the demand function factor:
        self.update_duration_storage_and_df_factors(month, year)

        # 4. Update parameters:
        self.update_coefs_and_params(month)

    def update_coefs_and_params(self, month):
        self.z2 = (self.coefs[8] * self.summer_months[month - 1]) + self.sum_of_constant_terms
        self.sigma = (1.0 / self.coefs[9])
        self.days_div_month_factor = 1.0
        self.sigma2 = (log(self.days_div_month_factor) - self.z2) / self.coefs[9]

    def update_and_apply_tariff_factor(self, year):
        tariff_factor_base = self.network.parameters.annual_tariff_factors.loc[
            (self.network.parameters.annual_tariff_factors["year"] == year),
            "commercial_tariff_factor"
        ].values[0]
        tariff_inflation_correction = self.network.exogenous_inputs.all_tariff_inflation_correction.loc[
            (self.network.exogenous_inputs.all_tariff_inflation_correction["year"] == year),
            "tariff_inflation_correction"
        ].values[0]
        tariff_factor = tariff_factor_base * tariff_inflation_correction

        tariff_changeyears = list(self.network.parameters.co_tariffs["change_years"])
        tariff_year_code = 1
        for i in tariff_changeyears:
            if year >= i:
                tariff_year_code += 1
        tariff_yeargovcodes = ((100 * tariff_year_code) + self.gov)
        tariff_structure = self.network.parameters.co_tariffs.loc[
            (self.network.parameters.co_tariffs["gov_code"] == tariff_yeargovcodes),
            "block_boundary":"block2_wastewater_price"
        ]
        self.tariff_blocks[0][1] = tariff_structure["block_boundary"].values[0]
        self.tariff_blocks[0][2] = float("inf")
        self.tariff_blocks[1] = ((self.sewage_rate * tariff_structure["block2_wastewater_price"].values[0]) +
                                 tariff_structure["block2_water_price"].values[0]) * tariff_factor
        self.tariff_blocks[2][0] = ((self.sewage_rate * tariff_structure["block1_wastewater_fixed"].values[0]) +
                                    tariff_structure["block1_water_fixed"].values[0]) * tariff_factor
        self.tariff_blocks[2][1] = ((self.sewage_rate * tariff_structure["block2_wastewater_fixed"].values[0]) +
                                    tariff_structure["block2_water_fixed"].values[0]) * tariff_factor

    def update_duration_storage_and_df_factors(self, month, year):
        use_supply_duration_factors = self.network.parameters.human['use_supply_duration_factors']
        if use_supply_duration_factors == 1:
            supply_duration_factor = self.network.parameters.supply_duration_factors.loc[
                (self.network.parameters.supply_duration_factors["year"] == year) &
                (self.network.parameters.supply_duration_factors["month"] == month) &
                (self.network.parameters.supply_duration_factors["subdist_code"] == self.subdist_code),
                "supply_duration_factors"
            ].values[0]
            self.duration = self.duration_base * supply_duration_factor
        if self.duration > 7.0:
            self.duration = 7.0

        ########################################################
        # Tanker analysis investment 2: Equitable distribution #
        ########################################################
        if self.network.simulation_type == "tanker":
            if ((self.network.parameters.tanker['tanker_analysis_investment_2'] == 1) and
                    (self.network.parameters.misc['supply_hours_equal'] == 'yes')):
                self.duration = 7.0

        use_storage_factors = self.network.parameters.co['use_commercial_storage_factors']
        if use_storage_factors == 1:
            storage_factor = self.network.parameters.storage_factors.loc[
                (self.network.parameters.storage_factors["year"] == year) &
                (self.network.parameters.storage_factors["month"] == month) &
                (self.network.parameters.storage_factors["subdist_code"] == self.subdist_code),
                "commercial_storage_factors"
            ].values[0]
            self.storage = self.storage_base * storage_factor
        if self.network.current_timestep_idx == 0:
            self.co_df_factor = self.network.parameters.co['demand_factor']

    def get_governorate(self):
        if self.governorate is None:
            govs = [i for i in self.network.institutions if i.component_type=='GovAgentWrapper']
            for g in govs:
                if g.gov_id == self.gov:
                    self.governorate = g
                    break
        return self.governorate

    def get_tariff_blocks(self):
        return self.tariff_blocks

    def get_admin_nrw_share_after_tech_nrw(self):
        self.get_governorate()
        return self.governorate.admin_nrw_share_after_tech_nrw

    def det_general_water_demand(self, price, filler_arg, month):
        if self.represented_units > 0:
            demand = exp((self.coefs[8] * self.summer_months[month - 1]) + (self.coefs[9] * price) +
                         self.sum_of_constant_terms)
        else:
            demand = 0.0
        return (self.co_df_factor * demand)

    def det_piped_water_demand(self, month):
        # 1. Determine whether the 2nd piped water block is relevant, based on the tanker price expectation (type):
        second_block_relevant = True
        tanker_price_expectation_type = self.network.parameters.tanker['tanker_price_expectation_type']
        tanker_price_expectation = self.tanker_prices[month - 1]
        if tanker_price_expectation_type == 1:
            timestep = self.network.current_timestep_idx + 1
            if (timestep <= 13) or (self.get_history("final_tanker_price")[-12] <= 0):
                self.tanker_price_expectation = self.tanker_prices[month - 1]
            else:
                tanker_price_expectation = (self.get_history("tanker_price_expectation")[-12] +
                                            self.get_history("final_tanker_price")[-12]) / 2.0
        if tanker_price_expectation_type == 2:
            tanker_price_expectation = 1e9
        self.tanker_price_expectation = tanker_price_expectation
        self.last_relevant_block_upper_bound = (self.tariff_blocks[0][2] / 91.25)
        if tanker_price_expectation < self.tariff_blocks[1]:
            second_block_relevant = False
            self.last_relevant_block_upper_bound = (self.tariff_blocks[0][1] / 91.25)

        # 2. Set up key parameters: (NRW and storage)
        admin_nrw_corr = self.get_admin_nrw_share_after_tech_nrw()
        effective_storage = (self.storage / max(1e-9, (7.0 - self.duration)))

        # 3. Determine hypothetical demand for the 2nd block: (accounting for fixed costs)
        hypothetical_demand = self.det_general_water_demand(self.tariff_blocks[1], 0, month)
        fixed_cost_block_difference = ((self.tariff_blocks[2][1] - self.tariff_blocks[2][0]) / 91.25)
        block_bound_avg_fixed_cost = max(0, (fixed_cost_block_difference /
                                             (hypothetical_demand -
                                              (self.tariff_blocks[0][1] / (91.25 * (1.0 - admin_nrw_corr))))))
        fixed_cost_for_demand_function = self.tariff_blocks[1] + (2.0 * block_bound_avg_fixed_cost)
        hypothetical_demand_with_fixed_cost = self.det_general_water_demand(fixed_cost_for_demand_function, 0, month)
        billed_hypothetical_demand = (1 - admin_nrw_corr) * hypothetical_demand
        billed_hypothetical_demand_with_fixed_cost = (1 - admin_nrw_corr) * hypothetical_demand_with_fixed_cost

        # 4. Determine whether the agent would enter the 2nd block or stay in the 1st one:
        if (min(billed_hypothetical_demand, billed_hypothetical_demand_with_fixed_cost) >
                (self.tariff_blocks[0][1] / 91.25)) and second_block_relevant:
            self.piped_demand = hypothetical_demand
        else:
            self.piped_demand = (self.tariff_blocks[0][1] / (91.25 * (1.0 - admin_nrw_corr)))

        # 5. Set storage-constrained piped demand for non-supply days:
        self.piped_non_supply_day_demand = min(effective_storage, self.piped_demand)

    def det_consumer_surplus_diff(self):
        month = self.network.current_timestep.month
        total_consumption = (self.piped_consumption + self.tanker_consumption)
        baseline_total_consumption = float(baseline_consumption_pickle[self.name][month - 1])
        baseline_represented_units = float(baseline_represented_units_pickle[self.name][month - 1])

        self.consumer_surplus_diff = \
            (0.0 if ((total_consumption <= 0.0) or (baseline_total_consumption <= 0.0)) else
             ((((self.sigma * log(total_consumption)) - self.sigma + self.sigma2) *
               total_consumption * self.represented_units) -
              (((self.sigma * log(baseline_total_consumption)) - self.sigma + self.sigma2) *
               baseline_total_consumption * baseline_represented_units) -
              self.expenditure + float(baseline_expenditure_pickle[self.name][month - 1])))

        self.consumer_surplus_diff_per_unit = \
            (0.0 if ((total_consumption <= 0.0) or (baseline_total_consumption <= 0.0)) else
             ((((self.sigma * log(total_consumption)) - self.sigma + self.sigma2) *
               total_consumption) -
              (((self.sigma * log(baseline_total_consumption)) - self.sigma + self.sigma2) *
               baseline_total_consumption) -
              (self.expenditure / self.represented_units) +
              (float(baseline_expenditure_pickle[self.name][month - 1]) / baseline_represented_units)
              ))  # 1.0))

        self.absolute_consumer_surplus = (0.0 if (total_consumption <= 0.0) else
                                          ((((self.sigma * log(total_consumption)) - self.sigma + self.sigma2) *
                                            total_consumption * self.represented_units) - self.expenditure))

    def get_marginal_willingness_to_pay(self):
        demand_function_quantity = (self.piped_consumption + self.tanker_consumption)
        return ((self.sigma * log(demand_function_quantity)) + self.sigma2)

    def setup(self, timestamp):
        if self.network.current_timestep.year <= 2015:
            self.update_params_historical()
        else:
            if self.network.current_timestep_idx == 0:
                self.update_params_historical()
            self.update_params_projection()


class HumanINAgent(Component):
    """
    A large industry agent class, implemented in relevant subdistricts. This is a basic PyNSim component, not a node.
    """
    description = "A large industry agent class, implemented in relevant subdistricts. Not a node."

    def __init__(self, name, gov, gov_name, subdist_number, subdist_code, subdist_name, x, y, industry_name, is_active,
                 piped_water_use, tariff, well_water_use, groundwater_cost, groundwater_node, surface_water_use, surface_water_cost,
                 water_value_per_m3, water_source_names, month_factors, **kwargs):
        super(HumanINAgent, self).__init__(name, **kwargs)
        self.name = name
        self.type = "industry"
        self.gov = gov
        self.gov_name = gov_name
        self.subdist_number = subdist_number
        self.subdist_code = subdist_code
        self.subdist_name = subdist_name
        self.governorate = None
        self.subdist = None
        self.x = x
        self.y = y
        self.represented_units = 1
        self.industry_name = industry_name
        self.is_active = is_active
        self.piped_water_use = piped_water_use
        self.tariff = tariff
        self.well_water_use = well_water_use
        self.groundwater_cost = groundwater_cost
        self.groundwater_node = groundwater_node
        self.surface_water_use = surface_water_use
        self.surface_water_cost = surface_water_cost
        self.water_value_per_m3 = water_value_per_m3
        self.water_source_names = water_source_names
        self.month_factors = month_factors

    _properties = {
        'piped_demand': 0,
        'piped_consumption': 0,
        'well_demand': 0,
        'well_consumption': 0,
        'surface_demand': 0,
        'surface_consumption': 0,
        'total_demand': 0,
        'total_consumption': 0,
        'piped_expenditure': 0,
        'well_expenditure': 0,
        'surface_expenditure': 0,
        'expenditure': 0,
        'water_value_lost': 0,
        'represented_units': 0,
    }

    def update_params(self):
        year = self.network.current_timestep.year
        if year > 2015:
            year = 2015
        year_subdist_row = ((year - 2006) * 89) + self.subdist_number
        subdistrict_params = self.network.exogenous_inputs.in_subdistrict_params.ix[year_subdist_row, :]
        self.is_active = subdistrict_params["is_active"]
        self.piped_water_use = subdistrict_params["piped_water_use"]
        self.well_water_use = subdistrict_params["well_water_use"]
        self.surface_water_use = subdistrict_params["surface_water_use"]
        self.water_value_per_m3 = subdistrict_params["water_value_per_m3"]

    def det_well_water_demand(self, month):
        if self.is_active == 1:
            demand = self.month_factors[month - 1] * self.well_water_use / 365.0
        else:
            demand = 0.0
        self.well_demand = self.network.parameters.id["industry_demand_factor"] * demand

        self.network.get_node(self.groundwater_node).pumping += self.well_demand / (24 * 60 * 60)
        if self.groundwater_node == '340201_urb_12':
            self.network.get_node(self.groundwater_node).pumping -= self.well_demand / (24 * 60 * 60)
            self.network.get_node('340202_urb_12').pumping += self.well_demand / (24 * 60 * 60)

    def det_surface_water_demand(self, month):
        if self.is_active == 1:
            demand = self.month_factors[month - 1] * self.surface_water_use / 365.0
        else:
            demand = 0.0
        self.surface_demand = self.network.parameters.id["industry_demand_factor"] * demand

    def det_piped_water_demand(self, month):
        if self.is_active == 1:
            demand = self.month_factors[month - 1] * self.piped_water_use / 365.0
        else:
            demand = 0.0
        self.piped_demand = self.network.parameters.id["industry_demand_factor"] * demand

    def det_water_value_lost(self):
        if (self.is_active == 1) and (self.total_demand > self.total_consumption):
            water_value_lost = self.water_value_per_m3 * (self.total_consumption - self.total_demand)
        else:
            water_value_lost = 0.0
        self.water_value_lost = water_value_lost

    def setup(self, timestamp):
        self.update_params()
