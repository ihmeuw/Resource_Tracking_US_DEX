## ==================================================
## Author(s): Max Weil
## Purpose: Python class to perform Das Gupta decomposition on pandas dataframes
## ==================================================

import numpy as np
import pandas as pd
import itertools
import string


class das_gupta_decomp:
    def __init__(self, N: int):

        # Checking if N exceeds 26
        if N > 26:
            raise ValueError(
                f"Cannon create decomposition equation for {N} factors. Up to 26 factors are currently supported."
            )

        # Setting up self-variables
        self.__N = N
        self.__R1_factors = list(string.ascii_uppercase)[:N]
        self.__R2_factors = list(string.ascii_lowercase)[:N]
        self._effect_eqs = self.get_equations()

    def __str__(self):
        return f"<Das Gupta Decomposition Object of {self._N} Factors>"

    def get_equations(self):
        # From number of factors, determining number of terms in Das Gupta decomposition equation
        terms = self.__N // 2 + self.__N % 2

        # Initializing dictionary containing equations
        effect_eqs = {}

        # For each decomp factor...
        for idx, factor in enumerate(self.__R1_factors):

            # Getting factors for each rate, not including the current factor being analyzed
            R1_facts = [elem for elem in self.__R1_factors if elem != factor]
            R2_facts = [elem for elem in self.__R2_factors if elem != factor.lower()]

            # Getting valid combinations of factors for both rates
            # Valid combinations are any combinations of R1 and R2 elements
            # Of length N-1 where all elements in that combination are non-duplicate letters
            # ex. 'B' and 'b' are considered duplicate letters
            combos = [
                combo
                for combo in itertools.combinations(R1_facts + R2_facts, self.__N - 1)
                if len(set([x.lower() for x in combo])) == self.__N - 1
            ]

            # Adding a count of the number of capital factors (need to change name)
            n_caps = {
                f"({'*'.join(combo)})": sum(1 for c in combo if c.isupper())
                for combo in combos
            }

            # Creating list to contain equation terms
            term_list = []

            # For each term in equation...
            for t in range(terms):

                # Getting term numerator
                numerator = "+".join(
                    [
                        k
                        for k, v in n_caps.items()
                        if v == (self.__N - (self.__N - t)) or v == (self.__N - (t + 1))
                    ]
                )

                # Getting term denominator
                denominator = str(
                    int(
                        np.prod([x for x in range(self.__N - t, self.__N + 1)])
                        / max(1, t)
                    )
                )

                # Adding term to term list
                term_list += [f"(({numerator})/({denominator}))"]

            # Creating equation and adding to equation dictionary
            equation = f"({'+'.join(term_list)})*({factor.lower()}-{factor})"
            # equation = f"({'+'.join(term_list)})"
            effect_eqs[f"{factor}_effect"] = equation

        return effect_eqs

    def decompose(
        self,
        base_df: pd.DataFrame,
        comp_df: pd.DataFrame,
        col_names: list = [],
        base_col_names: list = [],
        comp_col_names: list = [],
    ):

        # Raising error if rows are not equal
        if len(base_df) != len(comp_df):
            raise RuntimeError(
                "Dataframes must have the same number of rows. Decompositions are calculated for each row pair."
            )

        # Raising error if too many parameters are provided
        if not (col_names or base_col_names or comp_col_names):
            raise ValueError(
                "Must provide col_names OR base_col_names and comp_col_names."
            )
        elif col_names and (base_col_names or comp_col_names):
            raise ValueError(
                "Must only provide either col_names or base_col_names and comp_col_names."
            )
        elif not col_names and not (base_col_names and comp_col_names):
            raise ValueError(
                "Must only provide both base_col_names and comp_col_names if one is provided."
            )

        # Creating name_dict object to map from base_df to comp_df columns
        if base_col_names and comp_col_names:
            name_dict = dict(zip(base_col_names, comp_col_names))
        else:
            name_dict = dict(zip(col_names, col_names))

        # Raising error if the wrong number of columns were provided
        # Also throws error if duplicate column names are provided
        if len(name_dict) != self.__N:
            raise ValueError(
                f"Expecting to decompose {self.__N} columns but {len(name_dict)} were provided"
            )

        # Raising error if any columns provided are not present in either dataframe
        if not all([c in base_df.columns for c in name_dict.keys()]):
            raise KeyError("Some column names provided are not present in base_df.")
        elif not all([c in comp_df.columns for c in name_dict.values()]):
            raise KeyError("Some column names provided are not present in comp_df.")

        # Mapping variable names to corresponding columns
        var_to_name = {
            var: col
            for var, col in zip(
                self.__R1_factors + self.__R2_factors,
                list(name_dict.keys()) + list(name_dict.values()),
            )
        }
        var_to_col = {
            var: col
            for var, col in zip(
                self.__R1_factors + self.__R2_factors,
                [f"base_df['{i}'].values" for i in name_dict.keys()]
                + [f"comp_df['{i}'].values" for i in name_dict.values()],
            )
        }

        # Getting translated equations
        trans_effect_eqs = {
            f"{self._translate_string(eff[0], var_to_name)}_effect": self._translate_string(
                eq, var_to_col
            )
            for eff, eq in self._effect_eqs.items()
        }

        decomp_df = pd.DataFrame()
        for f, eq in trans_effect_eqs.items():
            decomp_df = pd.eval(f"{f}=" + eq, target=decomp_df, engine="python")

        return decomp_df

    def _translate_string(self, s, d):
        return "".join([d.get(x, x) for x in s])
