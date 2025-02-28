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

"""This module defines the ExogenousInputs class.

Attributes:
    exog_inputs_xlsx (pandas ExcelFile object): Pandas ExcelFile object of exog_inputs.xlsx
    population_inputs (pandas dataframe): Pandas dataframe of population inputs
    crop_quota_inputs (pandas dataframe): Pandas dataframe of crop quota inputs

"""

from JWM import get_excel_data, get_df_from_pickle
from JWM import basepath, excel_data_folder
import os
import datetime
import pandas as pd
import re

import logging
log = logging.getLogger(__name__)

exog_scenario_inputs_xlsx = get_excel_data("exog_scenario_inputs.xlsx")

exog_intervention_inputs_xlsx = get_excel_data("exog_intervention_inputs.xlsx")
exog_human_module_inputs_xlsx = get_excel_data("human_module_params.xlsx")


class ExogenousInputs(object):
    """A class (non-pynsim) to store and access all exogenous inputs for the Jordan model.

    The ExogenousInputs class stores inputs that define scenarios and interventions, and
    provides methods to access those inputs. Each pynsim simulation contains an ExogenousInputs
    object, which is stored as an attribute on the simulator's network. Agents should access
    exogenous inputs solely through the exogenous inputs class.

    Args:
        simulation (pynsim simulator object): object of class type JordanSimulator

    Attributes:
        scenario (string): scenario id
        intervention (string): intervention id
        scenario_inputs (dict): dictionary of scenario inputs (__init__ also sets each key as an attribute)
        intervention_inputs (dict): dictionary of intervention inputs (__init__ also sets each key as an attribute)


    """ 

    def __init__(self, simulation):
        self.scenario = simulation.scenario_id  # name of the scenario
        self.intervention = simulation.intervention_id  # name of the intervention
        self.csf_job = simulation.csf_job
        self._scenario_inputs = {
            'population': {},  # dictionary identified by subdistrict
            'cwr': {},
        }

        self._intervention_inputs = {
            'min_res_storage': {},
            'wehdah_release_target': {}
        }

        self._categories = {
            'farming_methods': ('open field', 'greenhouse'),
            'business_types': ('family', 'agricultural', 'mixed'),
            'crop_types': ('citrus', 'vegetables', 'bananas','other'),
            'water_types': ('fresh', 'blended'),
        }

        for k, v in self._scenario_inputs.items():
            setattr(self, k, v)

        for k, v in self._intervention_inputs.items():
            setattr(self, k, v)

        for k, v in self._categories.items():
            setattr(self, k, v)

    def load_scenario_data(self):

        log.info("Loading scenario data")
        for k, v in self._scenario_inputs.items():
            inputs = exog_scenario_inputs_xlsx.parse(k)
            name = "_" + k
            value = {}
            if inputs['index3_value'].count() != 0:
                value = dict((i, 0) for i in inputs[(inputs.scenario == self.scenario)]['index1_value'].values)
                for i1 in value.keys():
                    value[i1] = dict((i, 0) for i in inputs[(inputs.scenario == self.scenario)]['index2_value'].values)
                    for i2 in inputs[(inputs.scenario == self.scenario)]['index2_value'].unique():
                        value[i1][i2] = dict((i, 0) for i in inputs[(inputs.scenario == self.scenario)]['index3_value'].values)
                        for i3 in inputs[(inputs.scenario == self.scenario)]['index3_value'].unique():
                            try:
                                value[i1][i2][i3] = inputs[(inputs.index1_value == i1) & (inputs.index2_value == i2) & (inputs.index3_value == i3) & (inputs.scenario == self.scenario)]['value'].values[0]
                            except IndexError:
                                pass

            elif inputs['index2_value'].count() != 0:
                value = dict((i, 0) for i in inputs[(inputs.scenario == self.scenario)]['index1_value'].values)
                for i1 in value.keys():
                    value[i1] = dict((i, 0) for i in inputs[(inputs.scenario == self.scenario)]['index2_value'].values)
                    for i2 in inputs[(inputs.scenario == self.scenario)]['index2_value'].unique():
                        try:
                            value[i1][i2] = inputs[(inputs.index1_value == i1) & (inputs.index2_value == i2) & (inputs.scenario == self.scenario)]['value'].values[0]
                        except IndexError:
                            pass

            elif inputs['index1_value'].count() != 0:
                value = dict((i, 0) for i in inputs[(inputs.scenario == self.scenario)]['index1_value'].values)
                for i1 in value.keys():
                    try:
                        value[i1] = inputs[(inputs.index1_value == i1) & (inputs.scenario == self.scenario)]['value'].values[0]
                    except IndexError:
                        pass

            setattr(self, name, value)
        log.info("Scenario data loaded")

    def load_intervention_data(self):
        log.info("Loading intervention data")
        for k, v in self._intervention_inputs.items():
            inputs = exog_intervention_inputs_xlsx.parse(k)
            name = "_" + k
            value = {}
            if inputs['index3_value'].count() != 0:
                value = dict((i, 0) for i in inputs[(inputs.intervention == self.intervention)]['index1_value'].values)
                for i1 in value.keys():
                    value[i1] = dict((i, 0) for i in inputs[(inputs.intervention == self.intervention)]['index2_value'].values)
                    for i2 in inputs[(inputs.intervention == self.intervention)]['index2_value'].unique():
                        value[i1][i2] = dict((i, 0) for i in inputs[(inputs.intervention == self.intervention)]['index3_value'].values)
                        for i3 in inputs[(inputs.intervention == self.intervention)]['index3_value'].unique():
                            try:
                                value[i1][i2][i3] = inputs[(inputs.index1_value == i1) & (inputs.index2_value == i2) & (inputs.index3_value == i3) & (inputs.intervention == self.intervention)]['value'].values[0]
                            except IndexError:
                                pass

            elif inputs['index2_value'].count() != 0:
                value = dict((i, 0) for i in inputs[(inputs.intervention == self.intervention)]['index1_value'].values)
                for i1 in value.keys():
                    value[i1] = dict((i, 0) for i in inputs[(inputs.intervention == self.intervention)]['index2_value'].values)
                    for i2 in inputs[(inputs.intervention == self.intervention)]['index2_value'].unique():
                        try:
                            value[i1][i2] = inputs[(inputs.index1_value == i1) & (inputs.index2_value == i2) & (inputs.intervention == self.intervention)]['value'].values[0]
                        except IndexError:
                            pass

            elif inputs['index1_value'].count() != 0:
                value = dict((i, 0) for i in inputs[(inputs.intervention == self.intervention)]['index1_value'].values)
                for i1 in value.keys():
                    try:
                        value[i1] = inputs[(inputs.index1_value == i1) & (inputs.intervention == self.intervention)]['value'].values[0]
                    except IndexError:
                        pass

            setattr(self, name, value)
        log.info("Intervention data loaded")

    def load_human_model_data(self, simulation_type, simulation_number):

        # +++ Parameter input file selection: START +++
        # if self.csf_job is None:
        #     parameter_file_name = "parameter_inputs" + ".xlsx"
        # else:
        #     parameter_file_name = "parameter_inputs" + str(self.csf_job) + ".xlsx"
        # parameter_inputs_xlsx_human = get_excel_data(parameter_file_name).parse("human")
        parameter_file_name = (simulation_type + "_simulation_inputs\\" + "parameter_inputs" + str(simulation_number) +
                               ".xlsx")
        # logging.info("+++ Parameter file - human: " + simulation_type + " " + str(simulation_number) + " +++")
        parameter_inputs_xlsx_human = get_excel_data(parameter_file_name).parse("human")
        # +++ Parameter input file selection: END +++

        ssp_scenario_selected = \
            parameter_inputs_xlsx_human[(parameter_inputs_xlsx_human.parameter_name == "ssp_selection")][
                'value'].values[0]
        print "SSP scenario selected:  %s" % ssp_scenario_selected
        if ssp_scenario_selected == 1:
            exog_projections_SSP_xlsx = get_excel_data("SSP_inputs_2100-SSP1.xlsx")
        if ssp_scenario_selected == 2:
            exog_projections_SSP_xlsx = get_excel_data("SSP_inputs_2100-SSP2.xlsx")
        if ssp_scenario_selected == 3:
            exog_projections_SSP_xlsx = get_excel_data("SSP_inputs_2100-SSP3.xlsx")
        if ssp_scenario_selected == 4:
            exog_projections_SSP_xlsx = get_excel_data("SSP_inputs_2100-SSP4.xlsx")
        if ssp_scenario_selected == 5:
            exog_projections_SSP_xlsx = get_excel_data("SSP_inputs_2100-SSP5.xlsx")

        human_nrw = exog_human_module_inputs_xlsx.parse("hh_global_params")["nrw2_total"][0:120]
        hh_df_params = exog_human_module_inputs_xlsx.parse("hh_df_params")
        hh_subdistrict_params = exog_human_module_inputs_xlsx.parse("hh_subdistricts")
        rf_subdistrict_params = exog_human_module_inputs_xlsx.parse("rf_subdistricts")
        in_subdistrict_params = exog_human_module_inputs_xlsx.parse("in_subdistricts")
        co_populations = exog_human_module_inputs_xlsx.parse("co_populations")
        industry_supplies = exog_human_module_inputs_xlsx.parse("in_supplies")

        monthly_pop_etc = exog_projections_SSP_xlsx.parse("monthly_pop_etc")

        monthly_pop_etc_hist = monthly_pop_etc.loc[(monthly_pop_etc["scenario"] == "HIST"), :]
        monthly_pop_etc_ssp = monthly_pop_etc.loc[(monthly_pop_etc["scenario"] == ("SSP"+str(ssp_scenario_selected))),:]

        tanker_market_global_params = exog_human_module_inputs_xlsx.parse("tanker_market_global_params")
        all_tariff_inflation_correction = exog_human_module_inputs_xlsx.parse("all_tariff_inflation_correction")

        setattr(self, "human_nrw", human_nrw)
        setattr(self, "hh_df_params", hh_df_params)
        setattr(self, "hh_subdistrict_params", hh_subdistrict_params)
        setattr(self, "rf_subdistrict_params", rf_subdistrict_params)
        setattr(self, "in_subdistrict_params", in_subdistrict_params)
        setattr(self, "co_populations", co_populations)
        setattr(self, "industry_supplies", industry_supplies)
        setattr(self, "monthly_pop_etc_hist", monthly_pop_etc_hist)
        setattr(self, "monthly_pop_etc_ssp", monthly_pop_etc_ssp)
        setattr(self, "tanker_market_global_params", tanker_market_global_params)
        setattr(self, "all_tariff_inflation_correction", all_tariff_inflation_correction)

    def load_highland_farm_module_data(self, simulation):
        log.info('Set up Highland Farm Data')

        exog_farm_param = simulation.network.parameters.farms

        ## Load pyomo_inputs
        pyomo_input_dir = os.path.join(basepath, 'data', 'excel_data')  # PDS
        filepath = os.path.join(pyomo_input_dir, exog_farm_param['filename_pyomo_data'])
        data_file = pd.ExcelFile(filepath)
        simulation.network.parameters.farms['data_profit'] = data_file.parse('Profit')
        simulation.network.parameters.farms['data_constraints'] = data_file.parse('Constraints')
        _crop_types = ['Barley', 'OtherVegW', 'OtherFld', 'Olive', 'OtherTrees', 'OtherVegS']
        simulation.network.parameters.farms['crop_types'] = _crop_types
        simulation.network.parameters.farms['subdistricts'] = simulation.network.parameters.farms['data_profit']['subdistrict'][0:89].tolist()
        simulation.network.parameters.farms['crop_no'] = len(_crop_types)

        #A.2. Loading post-processing data, etc.:
        simulation.network.parameters.farms['data_seasons'] = data_file.parse('iCWDMNTH')

        # Tanker market initial conditions
        tanker_constraint_price = pd.read_csv(os.path.join(basepath, excel_data_folder, exog_farm_param['filename_tanker_abstr_const']))
        tanker_constraint_price = tanker_constraint_price.set_index('Subdistrict')

        ### Tanker market price and offer start conditions ###
        hfarms = simulation.network.get_nodes('HighlandFarmAgent')
        for hfarm in hfarms:
            _sub_dist = int(hfarm.subdistrict)
            hfarm.init_tanker_demand = tanker_constraint_price.loc[_sub_dist][-12:].to_dict()
            hfarm.tanker_well_capacity_initial = tanker_constraint_price["capacity_m3_mth"].loc[_sub_dist]
            hfarm.tanker_well_low_salinity_share = tanker_constraint_price["low_salinity_share"].loc[_sub_dist]

        ##### ##### ##### LOAD SCENARIO OPTIONS FILES AND SET SCENARIOS ##### ##### #####
        options_file_path = os.path.join(basepath, 'data', 'excel_data', exog_farm_param['filename_run_options'])
        xl_options_file = pd.ExcelFile(options_file_path)

        gw_abstr_factor_df = xl_options_file.parse("gw_constr")
        energy_prices_factor_df = xl_options_file.parse("energy_price")
        crop_prices_factor_df = xl_options_file.parse("crop_price")
        crop_water_df = xl_options_file.parse("crop_water")
        land_area_df = xl_options_file.parse("land_area")

        gw_abstr_factor_df.set_index(['Scenario', 'Year'], inplace=True)
        energy_prices_factor_df.set_index(['Scenario', 'Year'], inplace=True)
        crop_prices_factor_df.set_index(['Scenario', 'Year'], inplace=True)
        crop_water_df.set_index(['Scenario', 'Year'], inplace=True)
        land_area_df.set_index(['Scenario', 'Year'], inplace=True)

        exog_farm_param['gw_abstr_factors'] = gw_abstr_factor_df.loc[exog_farm_param['gw_abstr_scenario']]
        exog_farm_param['energy_price_factors'] = energy_prices_factor_df.loc[exog_farm_param['energy_price_scenario']]
        exog_farm_param['crop_price_factors'] = crop_prices_factor_df.loc[exog_farm_param['crop_price_scenario']]
        exog_farm_param['crop_water_factors'] = crop_water_df.loc[exog_farm_param['crop_water_scenario']]
        exog_farm_param['max_land_area_factors'] = land_area_df.loc[exog_farm_param['max_land_scenario']]

        ## Other for climate alt
        exog_farm_param['pCIRDELTA'] = data_file.parse('pCIRDELTA')
        exog_farm_param['aCIRDELTA'] = data_file.parse('aCIRDELTA')

        log.info('End Highland Farm Setup')

    def load_exogenous_inputs(self, simulation):
        """Call methods to add exogenous inputs to ExogenousInputs object.

        Args:
            simulation (pynsim simulator object): object of class type JordanSimulator

        Returns:
            (adds population inputs as an attribute to ExogenousInputs object)

        """

        self.add_population_properties(simulation)
        self.add_crop_quota()

    def setup(self, timestep):
        """Set attribute value for time period.

        Updates attribute values of exogenous input for time period, e.g. sets self.population based on
        self._population[timestep].

        Args:
            timestep (datetime object): model timestep as a datetime object

        Returns:
            (updates values of attributes, e.g. self.population)

        """
        for p in self._scenario_inputs.keys():
            attribute_name = '_' + p
            timestep_year = timestep.year
            if hasattr(self, attribute_name):
                attribute = getattr(self, attribute_name)
                try:
                    setattr(self, p, attribute[timestep_year])
                except:
                    # log.warn('%s not found for year %s',attribute_name,timestep_year)
                    pass


        for p in self._intervention_inputs.keys():
            attribute_name = '_' + p
            timestep_year = timestep.year
            if hasattr(self, attribute_name):
                attribute = getattr(self, attribute_name)
                try:
                    setattr(self, p, attribute[timestep_year])
                except:
                    # log.warn('%s not found for year %s',attribute_name,timestep_year)
                    pass






