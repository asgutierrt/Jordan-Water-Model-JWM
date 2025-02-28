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

import os
from JWM import basepath
from pyomo.environ import *
import pandas as pd
from datetime import datetime
pd.set_option('display.width', 1280)
pd.set_option('display.max_columns', 36)
import numpy as np
from pynsim import Engine
from copy import deepcopy
import logging
log = logging.getLogger('farmer_engine')
import calendar
import ast
import pyutilib.common
from pyomo.opt import SolverStatus, TerminationCondition


# +++ FARMER CROP PLANTING ENGINE +++
class HighlandFarmerPlantingEngine(Engine):
    name = """Highland Farmers determine crop planting, irrigation scheduling and tanker sales"""
    """ The target of this is an institution containing all highland farmer nodes """

    stand_alone_mode = False

    def initialise(self):
        self.exog_farm_param = self.target.network.parameters.farms
        self.rcp_choice = self.exog_farm_param['rcp_choice']
        self.crop_types = deepcopy(self.exog_farm_param['crop_types'])
        self.tanker_well_tax = self.exog_farm_param['gw_tanker_tax']
        self.data_profit = self.exog_farm_param['data_profit'].copy(deep=True)
        self.data_profit['crop_price_factor'] = 1
        self.data_profit['energy_price_factor'] = 1
        self.data_profit['nir_fc_delta'] = 0
        self.data_profit['nir_ac_delta'] = 0

        self.data_constraints = self.exog_farm_param['data_constraints'].copy(deep=True)
        self.data_seasons = self.exog_farm_param['data_seasons'].copy(deep=True)
        self.data_seasons['mth'] = self.data_seasons.apply(lambda r: calendar.month_abbr[r.month], axis=1)

        self.highland_farms = self.target.nodes
        self.subdistricts = [int(f.subdistrict) for f in self.highland_farms]
        self.subdistricts.sort()

        self.ids = self.data_profit.index.tolist()
        self.farm_ids = range(len(self.data_profit['subdistrict'].unique()))
        self.binary_ids = range(2)

        self.crop_ids_by_farm = {i: np.where(self.data_profit["subdistrict"] == sd)[0].tolist()
                                 for i, sd in enumerate(self.subdistricts)}
        self.crop_sd_by_farm = {sd: np.where(self.data_profit["subdistrict"] == sd)[0].tolist()
                                for i, sd in enumerate(self.subdistricts)}
        self.crop_ids_by_farm_and_constraint = {}
        self.land_constraints_by_farm = {}
        self.water_constraints_by_farm = {}

        self.swater_constraints_by_farm = {}
        self.total_water_constraints_by_farm = {}

        self.out_timesteps = ast.literal_eval(self.exog_farm_param['output_timesteps'])

        for i in range(89):
            for su in range(2):
                for rf in range(2):
                    self.crop_ids_by_farm_and_constraint[i, su, rf] = \
                    np.where((self.data_profit["subdistrict"] == self.subdistricts[i]) &
                             (self.data_profit["summer"] == su) &
                             (self.data_profit["rainfed"] == rf))[0].tolist()
                    constraint_ids = np.where((self.data_constraints["subdistrict"] == self.subdistricts[i]) &
                                              (self.data_constraints["summer"] == su) &
                                              (self.data_constraints["rainfed"] == rf))[0]
                    self.land_constraints_by_farm[i, su, rf] = float(
                        self.data_constraints.iloc[constraint_ids]["land_constraint"])
                    self.water_constraints_by_farm[i, su, rf] = float(
                        self.data_constraints.iloc[constraint_ids]["water_constraint"])
                    self.swater_constraints_by_farm[i, su, rf] = float(
                        self.data_constraints.iloc[constraint_ids]["surface_water_constraint"])

        # Setup output dataframe for model
        self.scn_id = 0
        self.solved_model = None
        self.df_output = None
        self.df_output = self.data_profit[['crop', 'subdistrict', 'summer', 'rainfed', 'price', 'yield', 'land_cost',
                                         'water_phead', 'water_pcf','water_nir','alpha', 'gw_nir', 'Observed_land_use',
                                         ]].copy(deep=True)
        self.df_output['crop_id'] = self.df_output.apply(lambda r: self.subdistricts.index(r.subdistrict)
                                                                   + self.crop_types.index(r.crop) * 89, axis=1)
        self.new_cols_for_output = ['scn_id', 'Observed_water_use', 'land_use', 'irrigation']
        for col in self.new_cols_for_output:
            self.df_output[col] = 0

        # Setup climate change parameters
        self.pCIRDELTA = self.exog_farm_param['pCIRDELTA'].copy(deep=True)
        self.aCIRDELTA = self.exog_farm_param['aCIRDELTA'].copy(deep=True)
        self.pCIRDELTA['pDelta'] = self.pCIRDELTA.apply(lambda r: {calendar.month_abbr[c]: r.loc[c] for c in range(1,13)}, axis=1)
        self.aCIRDELTA['aDelta'] = self.aCIRDELTA.apply(lambda r: {calendar.month_abbr[c]: r.loc[c] for c in range(1,13)}, axis=1)

        self.baseline_water_constraints_by_farm = deepcopy(self.water_constraints_by_farm)
        self.baseline_land_constraints_by_farm = deepcopy(self.land_constraints_by_farm)
        self.baseline_swater_constraints_by_farm = deepcopy(self.swater_constraints_by_farm)

        self.outputs_by_timestep = {}

    def run(self):
        logging.info("Farmer planning engine running")
        # Methods to update farm node properties
        if not self.stand_alone_mode:
            for farm in self.highland_farms:
                farm.get_pumping_lift()
                farm.get_wells_dry()
                if self.exog_farm_param['recalc_gw_pcnt'] == True:
                    farm.recalc_gw_percentage()
                farm.reset_groundwater_supply()

        # Select pyomo model
        if self.timestep_idx == 0:
            self.run_using_pyomo(force_winter_run=True)
        self.run_using_pyomo()

        # Methods to update farm node properties EVERY TIMESTEP
        if not self.stand_alone_mode:
            for farm in self.target.nodes:
                farm.set_tanker_offer_price()
                farm.set_irrigation_allocation()
                if int(self.exog_farm_param['endog_tanker_offer_calc']) == 1:
                    farm.set_tanker_offer_endogenous(self.exog_farm_param['tanker_supply_increase_factor'])
                else:
                    farm.set_tanker_offer_simple(self.exog_farm_param['tanker_supply_increase_factor'])

    def run_using_pyomo(self, force_winter_run=False):
        # Run ag decision and get data for relevant months
        if self.timestep.month in [1, 11]:
            logging.info('  .. for self.timestep.month in [1,11]')
            self.run_season = {1: 'summer', 11: 'winter'}[self.timestep.month]

            if force_winter_run:
                self.run_season = 'winter'

            # Update model scenario inputs
            self.set_constr_well_dry()
            self.set_model_pumping_lifts()

            # Set up scenario
            self.abstraction_limit_scenario()
            self.energy_price_scenario()
            self.crop_price_scenario()
            self.crop_irrigation_requirement_scenario()
            self.land_expansion_limit_scenario()

            # Get climate inputs from farm nodes and update
            if self.timestep_idx > 0 and self.timestep.year >= 2016 and \
                    self.exog_farm_param['rcp_choice'] != 0:
                self.get_climate_change_data_for_year()
                self.setup_nir_climate_change()
            else:
                self.data_profit['nir_fc_delta'] = 0
                self.data_profit['nir_ac_delta'] = 0

            # Methods to parameterise Pyomo model
            self.set_linear_profit_coef()

            self.total_water_constraints_by_farm = {}
            for k, v in self.water_constraints_by_farm.iteritems():
                self.total_water_constraints_by_farm[k] = v + self.swater_constraints_by_farm[k]

            # Run model
            self.Run_Pyomo_Model()
            if (self.solve_status == SolverStatus.ok) and (self.solve_termcond == TerminationCondition.optimal):
                self.get_seasonal_outputs()
                if not self.stand_alone_mode:
                    self.populate_farm_properties()
            else:
                logging.info('    .. model not solved')

            # Write Results to file
            if (self.timestep_idx in self.out_timesteps) or (self.timestep == self.target.network.timesteps[-1]):
                logging.info('    .. for output_timesteps')
                self.save_outputs_for_this_timestep()

            # Methods to update farm node properties IF CROPPING DECISION RUN
            for farm in self.highland_farms:
                farm.first_planning_run = True

    def set_constr_well_dry(self):
        logging.info('.. set_constr_well_dry()')
        for farm in self.highland_farms:
            if farm.well_reduction_pcnt == 0.0:
                _subdist = int(farm.subdistrict)
                _idx = self.subdistricts.index(_subdist)
                for k,v in self.water_constraints_by_farm.iteritems():
                    if k[0] == _idx:
                        self.water_constraints_by_farm[k] = 0.0

    def set_model_pumping_lifts(self):
        logging.info('.. set_pumping_lifts()')
        if self.timestep_idx == 0:
            if self.exog_farm_param['historical_or_future'].lower() == 'historical':
                self.data_profit['water_phead'] = self.data_profit['water_phead_2006']
            elif self.exog_farm_param['historical_or_future'].lower() == 'future':
                self.data_profit['water_phead'] = self.data_profit['water_phead_2015']
        else:
            self.data_profit['water_phead'] = self.data_profit.apply(lambda r: self.target.network.get_node(
                                                        str(r.subdistrict) + '_hfarm').pumping_lift, axis=1)

        self.data_profit['water_phead'] = self.data_profit.apply(lambda r: max(0.0, r.water_phead), axis=1)
        self.water_pheads = self.data_profit['water_phead']

    # SCENARIOS
    def abstraction_limit_scenario(self):
        if self.exog_farm_param['historical_or_future'].lower() == 'future':
            logging.info('.. abstraction_limit_scenario()')
            _gw_abstr_factors = self.exog_farm_param['gw_abstr_factors'].loc[self.timestep.year]
            self.data_profit.loc[self.data_profit.summer == 1, 'gw_abstr_factor'] = _gw_abstr_factors['summer']
            self.data_profit.loc[self.data_profit.summer == 0, 'gw_abstr_factor'] = _gw_abstr_factors['winter']

            for farm in self.highland_farms:
                _subdist = int(farm.subdistrict)
                _idx = self.subdistricts.index(_subdist)
                for k, v in self.water_constraints_by_farm.iteritems():
                    if k[0] == _idx:
                        self.water_constraints_by_farm[k] = self.baseline_water_constraints_by_farm[k] * \
                            farm.well_reduction_pcnt * _gw_abstr_factors[['winter', 'summer'][k[1]]]

        else:
            self.data_profit['gw_abstr_factor'] = 1.0
            self.water_constraints_by_farm = deepcopy(self.baseline_water_constraints_by_farm)

    def energy_price_scenario(self):
        logging.info('.. energy_price_scenario()')
        _energy_price_factors = self.exog_farm_param['energy_price_factors'].loc[self.timestep.year]
        self.data_profit['energy_price_factor'] = self.data_profit.apply(lambda r: _energy_price_factors[r.crop], axis=1)

    def crop_price_scenario(self):
        logging.info('.. crop_price_scenario()')
        _crop_price_factors = self.exog_farm_param['crop_price_factors'].loc[self.timestep.year]
        self.data_profit['crop_price_factor'] = self.data_profit.apply(lambda r: _crop_price_factors[r.crop], axis=1)

    def crop_irrigation_requirement_scenario(self):
        logging.info('.. crop_irrigation_requirement_scenario()')
        _crop_water_factor = self.exog_farm_param['crop_water_factors'].loc[self.timestep.year]
        self.data_profit['crop_water_factor'] = self.data_profit.apply(lambda r: _crop_water_factor[r.crop], axis=1)

    def land_expansion_limit_scenario(self):
        if self.exog_farm_param['historical_or_future'].lower() == 'future':
            logging.info('.. land_expansion_limit_scenario()')
            _max_land_factor = self.exog_farm_param['max_land_area_factors'].loc[self.timestep.year]
            self.data_profit.loc[self.data_profit.summer == 1, 'max_land_factor'] = _max_land_factor['summer']
            self.data_profit.loc[self.data_profit.summer == 0, 'max_land_factor'] = _max_land_factor['winter']
            for k, v in self.land_constraints_by_farm.iteritems():
                self.land_constraints_by_farm[k] =  \
                    self.baseline_land_constraints_by_farm[k]*_max_land_factor[['winter','summer'][k[1]]]
        else:
            self.land_constraints_by_farm = deepcopy(self.baseline_land_constraints_by_farm)
            self.data_profit['max_land_factor'] = 1.0

    def get_climate_change_data_for_year(self):
        _rcp = float(self.exog_farm_param['rcp_choice'])
        if _rcp in [4.5, 8.5]:
            _rcp = _rcp*10
        if _rcp in [45, 85]:
            _data_year = self.timestep.year
        else:
            _data_year = 2016
        self.pCC = self.pCIRDELTA.loc[(self.pCIRDELTA.Year == _data_year) &
                                      (self.pCIRDELTA.RCP == _rcp), ['Subdist','pDelta']]
        self.pCC.index = self.pCC.Subdist
        self.aCC = self.aCIRDELTA.loc[(self.aCIRDELTA.Year == _data_year) &
                                      (self.pCIRDELTA.RCP == _rcp), ['Subdist','aDelta']]
        self.aCC.index = self.aCC.Subdist

    def setup_nir_climate_change(self):
        logging.info('.. setup_nir_climate_change()')
        self.nir_calc = self.data_profit[['crop', 'subdistrict', 'summer', 'rainfed', 'water_nir']].copy(deep=True)
        self.nir_calc['irrig_pc'] = self.nir_calc.apply(lambda r:
                dict(self.data_seasons.loc[self.data_seasons.crop == r.crop, ['mth','water_percent']].values), axis=1)
        self.nir_calc['nir_mnth'] = self.nir_calc.apply(lambda r:
                                        {k: r.water_nir * v for k, v in r.irrig_pc.iteritems()}, axis=1)
        self.nir_calc['delta_fc'] = self.nir_calc.apply(lambda r: self.pCC.loc[r.subdistrict, 'pDelta'], axis=1)
        self.nir_calc['delta_ac'] = self.nir_calc.apply(lambda r: self.aCC.loc[r.subdistrict, 'aDelta'], axis=1)
        self.nir_calc['nir_fc_mnth'] = self.nir_calc.apply(lambda r:
                                        {k: max(0, r.nir_mnth[k] + r.delta_fc[k]) for k in r.nir_mnth.keys()}, axis=1)
        self.nir_calc['nir_ac_mnth'] = self.nir_calc.apply(lambda r:
                                        {k: max(0, r.nir_mnth[k] + r.delta_ac[k]) for k in r.nir_mnth.keys()}, axis=1)
        self.nir_calc['nir_fc'] = self.nir_calc.apply(lambda r: sum(r.nir_fc_mnth.values()), axis=1)
        self.nir_calc['nir_ac'] = self.nir_calc.apply(lambda r: sum(r.nir_ac_mnth.values()), axis=1)
        self.nir_calc['nir_fc_delta'] = self.nir_calc['nir_fc'] - self.nir_calc['water_nir']
        self.nir_calc['nir_ac_delta'] = self.nir_calc['nir_ac'] - self.nir_calc['water_nir']
        self.data_profit['nir_fc_delta'] = self.nir_calc['nir_fc_delta']
        self.data_profit['nir_ac_delta'] = self.nir_calc['nir_ac_delta']

    def set_linear_profit_coef(self):
        logging.info('.. def_linear_profit_coef()')
        self.prices = self.data_profit['price']
        self.yields = self.data_profit['yield']
        self.land_costs = self.data_profit['land_cost']
        self.water_pcfs = self.data_profit['water_pcf']
        self.water_nirs = self.data_profit['water_nir']
        self.gw_nirs = dict(self.data_profit['gw_nir'])
        self.alphas = self.data_profit['alpha']
        self.water_pheads = self.data_profit['water_phead']

        self.energy_price_factor = self.data_profit['energy_price_factor']
        self.crop_price_factor = self.data_profit['crop_price_factor']
        self.crop_water_factor = self.data_profit['crop_water_factor']

        self.nir_fc_deltas = self.data_profit['nir_fc_delta']
        self.nir_ac_deltas = self.data_profit['nir_fc_delta']

        self.water_nirs_adj = self.data_profit.apply(lambda r: max(r.crop_water_factor * (r.water_nir + r.nir_fc_delta),
                                                                   0.0), axis=1)
        self.gw_nirs_adj = self.data_profit.apply(lambda r: max(r.crop_water_factor * (r.water_nir + r.nir_fc_delta) *
                                                                ((r.gw_nir/r.water_nir) if r.water_nir > 0.0 else 0.0),
                                                                0.0), axis=1)

        self.linear_term_simple = [prc * yld - cst - phd * pcf * nir - alp for prc, yld, cst, phd, pcf, nir, alp in
                                zip(self.prices, self.yields, self.land_costs, self.water_pheads, self.water_pcfs,
                                    self.water_nirs, self.alphas)]

        self.linear_term_sum = [cpf*prc*yld - cst - epf * phd * pcf * max(cw*nir + dnir,0.0) - alp
                                for prc, yld, cst, phd, pcf, nir, alp, cpf, epf, dnir, cw in
                                zip(self.prices, self.yields, self.land_costs, self.water_pheads, self.water_pcfs,
                        self.water_nirs, self.alphas, self.crop_price_factor, self.energy_price_factor,
                                    self.nir_fc_deltas, self.crop_water_factor)]

        self.net_prices = dict(enumerate(self.linear_term_sum))
        self.x_start_values = dict(enumerate([0.0] * 3))

    def Run_Pyomo_Model(self):
        logging.info('.. Run_Pyomo_Model()')
        # C.1. Constructing model inputs:
        fwm = ConcreteModel()
        fwm.ids = Set(initialize=self.ids)
        fwm.farm_ids = Set(initialize=self.farm_ids)
        fwm.binary_ids = Set(initialize=self.binary_ids)
        fwm.crop_ids_by_farm = Set(fwm.farm_ids, initialize=self.crop_ids_by_farm)
        fwm.crop_ids_by_farm_and_constraint = Set(fwm.farm_ids, fwm.binary_ids, fwm.binary_ids,
                                                  initialize=self.crop_ids_by_farm_and_constraint)
        fwm.net_prices = Param(fwm.ids, initialize=self.net_prices, mutable=True)
        fwm.gammas = Param(fwm.ids, initialize=self.data_profit.gamma.to_dict(), mutable=True)
        fwm.land_constraints = Param(fwm.farm_ids, fwm.binary_ids, fwm.binary_ids,
                                          initialize=self.land_constraints_by_farm, mutable=True)
        fwm.water_constraints = Param(fwm.farm_ids, fwm.binary_ids, fwm.binary_ids,
                                      initialize=self.total_water_constraints_by_farm, mutable=True)
        fwm.nirs = Param(fwm.ids, initialize=self.water_nirs.to_dict(), mutable=True)
        fwm.nirs_adj = Param(fwm.ids, initialize=self.water_nirs_adj.to_dict(), mutable=True)
        fwm.gw_nirs_adj = Param(fwm.ids, initialize=self.gw_nirs_adj.to_dict(), mutable=True)
        fwm.xs = Var(fwm.ids, domain=NonNegativeReals, initialize=self.x_start_values)

        # Scenario Scalars
        fwm.abst_pc = Param(default=1.0)

        # C.2. Constructing model functions:
        def obj_fun(fwm):
            return 0.00001 * sum(sum(
                (fwm.net_prices[i] * fwm.xs[i] - 0.5 * fwm.gammas[i] * fwm.xs[i] * fwm.xs[i]) for i in
                fwm.crop_ids_by_farm[f]) for f in fwm.farm_ids)

        fwm.obj_f = Objective(rule=obj_fun, sense=maximize)

        def land_constraint(fwm, ff, rf):
            return (sum(fwm.xs[i] for i in fwm.crop_ids_by_farm_and_constraint[ff, 0, rf]) +
                    sum(fwm.xs[i] for i in fwm.crop_ids_by_farm_and_constraint[ff, 1, rf])) <= \
                   fwm.land_constraints[ff, 0, rf]
        fwm.c1 = Constraint(fwm.farm_ids, fwm.binary_ids, rule=land_constraint)

        def water_constraint(fwm, ff, rf):
            return (sum(fwm.xs[i] * fwm.nirs_adj[i] for i in fwm.crop_ids_by_farm_and_constraint[ff, 0, rf]) +
                    sum(fwm.xs[i] * fwm.nirs_adj[i] for i in fwm.crop_ids_by_farm_and_constraint[ff, 1, rf])) <= \
                   fwm.water_constraints[ff, 0, rf]
        fwm.c2 = Constraint(fwm.farm_ids, fwm.binary_ids, rule=water_constraint)

        # Creating and running the solver:
        opt = SolverFactory("ipopt", solver_io='nl')

        try:
            results = opt.solve(fwm, keepfiles=False, tee=False)
        except (ValueError, pyutilib.common._exceptions.ApplicationError) as error:
            logging.info("ipopt failed to converge ... retrying")
            try:
                results = opt.solve(fwm, keepfiles=False, tee=False)
            except (ValueError, pyutilib.common._exceptions.ApplicationError) as error:
                logging.info("ipopt failed to converge")

        self.solve_status = results.solver.status
        self.solve_termcond = results.solver.termination_condition
        self.solved_model = fwm


        logging.info(' #### IPOPT RUN ####')

        return fwm


    def get_seasonal_outputs(self):
        logging.info('.. get_seasonal_outputs()')
        if self.solved_model:
            self.result_xs = dict(self.solved_model.xs.get_values())
            self.df_output['scn_id'] = self.scn_id
            self.df_output['land_use'] = self.df_output.apply(
                lambda r: self.result_xs[r.crop_id] if self.result_xs[r.crop_id] else 0.0, axis=1)
            self.df_output['price'] = self.prices
            self.df_output['yield'] = self.yields
            self.df_output['water_phead'] = self.water_pheads
            self.df_output['water_pcf'] = self.water_pcfs
            self.df_output['water_nir'] = self.water_nirs

            self.df_output['gw_nir'] = self.data_profit['gw_nir']

            self.df_output['nir_fc_delta'] = self.data_profit['nir_fc_delta']
            self.df_output['nir_ac_delta'] = self.data_profit['nir_ac_delta']

            self.df_output['net_prices'] = self.net_prices.values()

            self.df_output['crop_price_factor'] = self.crop_price_factor
            self.df_output['energy_price_factor'] = self.energy_price_factor
            self.df_output['gw_abstr_factor'] = self.data_profit['gw_abstr_factor']
            self.df_output['crop_water_factor'] = self.crop_water_factor

            self.df_output['nir_fc_deltas'] = self.nir_fc_deltas
            self.df_output['nir_ac_deltas'] = self.nir_ac_deltas

            self.water_costs_fc = [epf * phd * pcf * max(nir*cw + dnir, 0.0) for phd, pcf, nir, epf, dnir, cw in
                                    zip(self.water_pheads, self.water_pcfs, self.water_nirs,
                                        self.energy_price_factor, self.nir_fc_deltas, self.crop_water_factor)]
            self.water_costs_ac = [epf * phd * pcf * max(nir*cw + dnir, 0.0) for phd, pcf, nir, epf, dnir, cw in
                                    zip(self.water_pheads, self.water_pcfs, self.water_nirs,
                                        self.energy_price_factor, self.nir_ac_deltas, self.crop_water_factor)]
            self.df_output['wcost_prj'] = self.water_costs_fc
            self.df_output['wcost_act'] = self.water_costs_ac

            self.df_output['nir_water_fc_adj'] = self.water_nirs_adj
            self.df_output['nir_water_ac_adj'] = self.df_output.apply(lambda r:
                                        max(r.crop_water_factor * (r.water_nir + r.nir_ac_delta), 0), axis=1)
            self.df_output['nir_gw_fc_adj'] = self.gw_nirs_adj
            self.df_output['nir_gw_ac_adj'] = self.df_output.apply(lambda r:
                                        max(r.crop_water_factor * (r.water_nir + r.nir_ac_delta) *
                                            ((r.gw_nir / r.water_nir) if r.water_nir > 0.0 else 0.0), 0.0), axis=1)
            self.df_output['irrigation'] = self.df_output.apply(lambda r: r.nir_water_fc_adj * r.land_use, axis=1)
            self.df_output['irrigation_act'] = self.df_output.apply(lambda r: r.nir_water_ac_adj * r.land_use, axis=1)
            self.df_output['gw_irrigation'] = self.df_output.apply(lambda r: r.nir_gw_fc_adj * r.land_use, axis=1)
            self.df_output['gw_irrigation_act'] = self.df_output.apply(lambda r: r.nir_gw_ac_adj * r.land_use, axis=1)

            self.df_output['profit_prj'] = self.df_output.apply(lambda r: r.land_use * (r.net_prices + r.alpha), axis =1)
            self.df_output['profit_act'] = self.df_output.apply(lambda r: r.profit_prj +
                                                     (r.wcost_act - r.wcost_prj)*r.land_use , axis =1)

            self.df_output['unit_pumping_cost'] = \
                self.df_output.apply(lambda r: r.energy_price_factor * r.water_pcf * r.water_phead, axis=1)
            
            self.df_output['opportunity_cost'] = self.df_output.apply(lambda r: 0.0 if (r.nir_water_fc_adj <= 0) else
                (r.crop_price_factor * r.price * r['yield'] - r.land_cost) / r.nir_water_fc_adj, axis=1)

            self.df_output['tanker_well_tax'] = self.tanker_well_tax
            self.df_output['current_reservation_price'] = self.df_output.apply(lambda r:  r.tanker_well_tax
                                                            + max(0.0, r.unit_pumping_cost, r.opportunity_cost), axis=1)
            self.df_output['subdistrict_reservation_price'] = -1

            def def_subdist_rsvn_price(row):
                return self.df_output.loc[(self.df_output.subdistrict == row.subdistrict)
                        & (self.df_output.summer == ['winter','summer'].index(self.run_season)), 'current_reservation_price'].min()

            self.df_output['subdistrict_reservation_price'] = self.df_output.apply(lambda  r: def_subdist_rsvn_price(r), axis=1)

            self.data_profit['linear_term_simple'] = self.linear_term_simple
            self.data_profit['linear_term_sum'] = self.linear_term_sum

        return self.df_output


    def populate_farm_properties(self):
        logging.info('.. populate_farm_properties()')
        for farm in self.highland_farms:
            subdist = int(farm.subdistrict)
            sd = self.subdistricts.index(subdist)
            for crop in self.crop_types:
                crp = self.crop_types.index(crop)
                crop_id = self.crop_ids_by_farm[sd][crp]
                farm.cropping[crop] = self.result_xs[crop_id] if self.result_xs[crop_id] else 0.0
                reservation_price = float(self.df_output.loc[(self.df_output['crop'] == crop) &
                                        (self.df_output['subdistrict'] == subdist),'subdistrict_reservation_price'])

            irrig_current_season = self.df_output.loc[(self.df_output.subdistrict == subdist) &
                                                      (self.df_output.summer == ['winter', 'summer'].index(self.run_season))
                                                     , ['crop','irrigation', 'irrigation_act']]
            irrig_current_season['mnt_pct'] = irrig_current_season.apply(lambda r: dict(self.data_seasons.loc[
                                            self.data_seasons.crop == r.crop, ['mth','water_percent']].values),axis=1)
            irrig_current_season['irrig_fc_mnth'] = irrig_current_season.apply(lambda r: {k: v * float(r.irrigation)
                                                            for k, v in r.mnt_pct.iteritems()}, axis=1)
            irrig_current_season['irrig_ac_mnth'] = irrig_current_season.apply(lambda r: {k: v * float(r.irrigation_act)
                                                            for k, v in r.mnt_pct.iteritems()}, axis=1)

            irrig_mth_fc = {calendar.month_abbr[m]: 0 for m in range(1,13)}
            for k,v in irrig_current_season['irrig_fc_mnth'].iteritems():
                for m in irrig_mth_fc.keys():
                    irrig_mth_fc[m] += v[m]
            irrig_mth_ac = {calendar.month_abbr[m]: 0 for m in range(1,13)}
            for k,v in irrig_current_season['irrig_ac_mnth'].iteritems():
                for m in irrig_mth_ac.keys():
                    irrig_mth_ac[m] += v[m]

            if self.run_season == 'summer':
                farm.irrig_supply_fc_summer = irrig_mth_fc
                farm.irrig_supply_ac_summer = irrig_mth_ac
                farm.reservation_price_fc_summer = {k: reservation_price for k in farm.reservation_price_fc_summer}
            elif self.run_season == 'winter':
                farm.irrig_supply_fc_winter = irrig_mth_fc
                farm.irrig_supply_ac_winter = irrig_mth_ac
                farm.reservation_price_fc_winter = {k: reservation_price for k in farm.reservation_price_fc_winter}

            farm.irrig_supply_fc_annual = {k: farm.irrig_supply_fc_summer[k] + farm.irrig_supply_fc_winter[k]
                                                for k in farm.irrig_supply_fc_summer.keys()}
            farm.irrig_supply_ac_annual = {k: farm.irrig_supply_ac_summer[k] + farm.irrig_supply_ac_winter[k]
                                                for k in farm.irrig_supply_ac_summer.keys()}
            gw_irrig_current_season = self.df_output.loc[(self.df_output.subdistrict == subdist) &
                                                (self.df_output.summer == ['winter', 'summer'].index(self.run_season)),
                                                         ['crop','gw_irrigation', 'gw_irrigation_act']]
            gw_irrig_current_season['mnt_pct'] = gw_irrig_current_season.apply(lambda r: dict(self.data_seasons.loc[
                                              self.data_seasons.crop == r.crop, ['mth','water_percent']].values),axis=1)
            gw_irrig_current_season['gw_irrig_fc_mnth'] = gw_irrig_current_season.apply(lambda r: {k: v *
                                                  float(r.gw_irrigation) for k, v in r.mnt_pct.iteritems()}, axis=1)
            gw_irrig_current_season['gw_irrig_ac_mnth'] = gw_irrig_current_season.apply(lambda r: {k: v *
                                                  float(r.gw_irrigation_act) for k, v in r.mnt_pct.iteritems()}, axis=1)

            gw_irrig_mth_fc = {calendar.month_abbr[m]: 0 for m in range(1,13)}
            for k,v in gw_irrig_current_season['gw_irrig_fc_mnth'].iteritems():
                for m in irrig_mth_fc.keys():
                    gw_irrig_mth_fc[m] += v[m]
            gw_irrig_mth_ac = {calendar.month_abbr[m]: 0 for m in range(1,13)}
            for k,v in gw_irrig_current_season['gw_irrig_ac_mnth'].iteritems():
                for m in irrig_mth_ac.keys():
                    gw_irrig_mth_ac[m] += v[m]

            if self.run_season == 'summer':
                farm.gw_irrig_supply_fc_summer = gw_irrig_mth_fc
                farm.gw_irrig_supply_ac_summer = gw_irrig_mth_ac
            elif self.run_season == 'winter':
                farm.gw_irrig_supply_fc_winter = gw_irrig_mth_fc
                farm.gw_irrig_supply_ac_winter = gw_irrig_mth_ac

            farm.gw_irrig_supply_fc_annual = {k: farm.gw_irrig_supply_fc_summer[k] + farm.gw_irrig_supply_fc_winter[k]
                                                  for k in farm.gw_irrig_supply_fc_summer.keys()}
            farm.gw_irrig_supply_ac_annual = {k: farm.gw_irrig_supply_ac_summer[k] + farm.gw_irrig_supply_ac_winter[k]
                                                  for k in farm.gw_irrig_supply_ac_summer.keys()}


            farm.reservation_price_fc_annual = {k: min(
                farm.reservation_price_fc_summer[k] if farm.reservation_price_fc_summer[k] > 0 else
                farm.reservation_price_fc_winter[k],
                farm.reservation_price_fc_winter[k] if farm.reservation_price_fc_winter[k] > 0 else
                farm.reservation_price_fc_summer[k])
                                                for k in farm.reservation_price_fc_summer.keys()}

            farm.reservation_price_fc_annual = {k: max(self.tanker_well_tax, v) for k, v in
                                                farm.reservation_price_fc_annual.iteritems()}

            # Subdistrict properties (not by crop type)
            farm.total_land_use = sum(farm.cropping.values())
            farm.cropping_profits_proj =  self.df_output[self.df_output.subdistrict == subdist]['profit_prj'].sum()
            farm.cropping_profits = self.df_output[self.df_output.subdistrict == subdist]['profit_act'].sum()

            farm.pcf = max(self.data_profit[self.data_profit.subdistrict == int(farm.subdistrict)]['water_pcf'])

    def save_outputs_for_this_timestep(self):
        logging.info(".. saving outputs for this timestep in s.engines[4].outputs_by_timestep")
        self.outputs_by_timestep[self.timestep_idx] = self.df_output

