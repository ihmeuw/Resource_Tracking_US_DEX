// No location model (without year)
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
  
  printf("MKL threads (mkl_get_max_threads): %d\n", mkl_get_max_threads());
  
  // Define data and inputs - these already exist (the inputs)
  // --------------------------------------------------------------
  DATA_STRING(link);  // link function - "poisson" or "gaussian"
  DATA_VECTOR(Y);     // value
  DATA_VECTOR(N);     // n_obs
  DATA_VECTOR(SE);    // standard error of the data point
  DATA_MATRIX(X);     // covariates (at minimum, a column for the intercept) 
  DATA_IVECTOR(A);    // age indicator
  DATA_IVECTOR(T);    // year indicator
  DATA_IVECTOR(D);    // data source indicator
  
  DATA_SPARSE_MATRIX(graph_a); 
  DATA_SPARSE_MATRIX(graph_t); 
  
  
  DATA_INTEGER(num_d);
  DATA_INTEGER(num_j);   // number of locations for IID
  DATA_INTEGER(rho_t_alpha); // = 20
  DATA_INTEGER(rho_t_beta); // = 2
  DATA_INTEGER(rho_a_alpha); // = 20
  DATA_INTEGER(rho_a_beta); // = 2
  DATA_INTEGER(rho_j_alpha); // = 20
  DATA_INTEGER(rho_j_beta); // = 2
  DATA_INTEGER(sigma_alpha); //= 1
  DATA_INTEGER(sigma_beta); //= 4
  // Define parameters
  // --------------------------------------------------------------
  Type nll = 0; //start with nll of zero, then all effects get added to this 
  
  // fixed effects (intercept, covariate effects)
  PARAMETER_VECTOR(B);

  
  // RE1: LCAR(age):IID(dataset)
  PARAMETER(re1_log_sigma);
  Type sigma_1 = exp(re1_log_sigma);
  PARAMETER(logit_rho_1a);
  Type rho_1a = invlogit(logit_rho_1a);
  nll -= dgamma(sigma_1, Type(sigma_alpha), Type(sigma_beta), true);
  nll -= dbeta(rho_1a, Type(rho_a_alpha), Type(rho_a_beta), true);

  
  PARAMETER_ARRAY(re1);
  
  // Evaluate random effects, and add to NLL
  // --------------------------------------------------------------

  // RE1: LCAR(age):IID(dataset)
  SparseMatrix<Type> K_1a = lcar_strmat(graph_a, rho_1a);
  
  // Create sparse identity matrix for IID data source effects
  matrix<Type> Sigma_d(num_d, num_d);
  Sigma_d.setIdentity();
  Eigen::SparseMatrix<Type> Sigma_Sparse_d = asSparseMatrix(Sigma_d);
  REPORT(Sigma_Sparse_d);
  nll += SCALE(SEPARABLE(GMRF(Sigma_Sparse_d), GMRF(K_1a)), sigma_1)(re1);

  // Add to nll
  nll -= dnorm(logit_rho_1a, Type(0), Type(1.5), true);
  
  // Evaluate data effects, and add to NLL
  // --------------------------------------------------------------
  
  // given data, how likely are our parameters
  // for each data point, construct our prediction and calculate the corresponding conditional probability of the data point using the chosen (here poisson) distribution
  
  if (link == "poisson") {
    // construct predictions of log m
    vector<Type> log_m = X * B; 
    for(size_t i = 0; i < Y.size(); i++)
      log_m[i] += re1(A[i], D[i]);
    vector<Type> m = exp(log_m); 
    
    // get corresponding conditional likelihood
    for(size_t i = 0; i < Y.size(); i++)
      nll -= dpois(Y[i], N[i]*m[i], true); // for poisson distribution
    
  }else{
    // construct predictions of log mu
    vector<Type> log_mu = X * B;
    for(size_t i = 0; i < Y.size(); i++)
      log_mu[i] += re1(A[i], D[i]) ;
    vector<Type> mu = exp(log_mu); 
    
    // data likelihood
    for(size_t i = 0; i < Y.size(); i++) 
      nll -= dnorm(Y[i], mu[i], SE[i], true);
    
  }
  
  return nll;
}
