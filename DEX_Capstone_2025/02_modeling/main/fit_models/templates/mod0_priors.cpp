// Intercept model
// 
// Authors: Azalea Thomson and Haley Lescinsky

#include <TMB.hpp>
#include </FILEPATH/mkl.h>
using namespace density;
using Eigen::SparseMatrix; 
#include "lcar_strmat.hpp"
#include "prior_param_struct.hpp"
#include "prior_penalty.hpp"

// Objective function
template<class Type>

Type objective_function<Type>::operator() () {
  DATA_STRING(link);  
  DATA_VECTOR(Y);
  DATA_VECTOR(N);
  DATA_VECTOR(SE);    
  PARAMETER(B);
  PARAMETER_VECTOR(iid_error);
  
  Type jnll = 0;
  
  // Probability of parameters given priors
  jnll -= dnorm(iid_error.sum(), Type(0.0), Type(1.0), true);
  
  // Probability of data given parameters
  if (link == "poisson") {
    vector<Type> log_risk = B + iid_error;
    for(int i = 0; i < Y.size(); i++){
      jnll -= dpois(Y[i], N[i] * exp(log_risk[i]), true);
    }
  }else{ 
    // Normal distribution
    vector<Type> log_risk = B + iid_error;
    for(int i = 0; i < Y.size(); i++){
      jnll -= dnorm(Y[i], exp(log_risk)[i], SE[i], true);
    }
  }
  
  return jnll;
}   
