// Function for preparing the log density of a prior on the log standard deviation
template<class Type>
  Type eval_prior_sigma(prior_type_sigma<Type> prior, Type log_sigma){
    Type penalty;
    // calculate the precision and log precision
    Type tau = pow(exp(log_sigma), -2.);
    Type logtau = log(tau);
    
    // The following documentation is helpful in understanding these derivations: https://becarioprecario.bitbucket.io/inla-gitbook/ch-priors.html#sec:priors
    
    if(prior.name == "pc") {
      // PENALIZED COMPLEXITY prior
      // the INLA documentation describes the prior expression for a PC prior: https://becarioprecario.bitbucket.io/inla-gitbook/ch-priors.html#sec:pcpriors
      // prior.par1 = sigma_0
      // prior.par2 = alpha
      // DERIVATION:
        // tau = f(theta) = 1/(exp(log(sigma))^2) = exp(log(sigma))^(-2) = exp(-2log(sigma)) = exp*-2theta)
// the first derivative of f(theta) = -2(exp(-2theta)) = -2tau
// from the INLA documentation: density(theta) = (lambda/2*tau^(-3/2)*exp(-lambda*tau^(-1/2)))(-2tau)
// once this is log transformed and the additive constants are dropped, we get:
  // -0.5logtau - lambda*tau^(-1.2) = -0.5logtau - lambda(exp(-0.5logtau))
  Type lambda = - log(prior.par2) / prior.par1;
  penalty = -lambda * exp(-logtau/Type(2.0)) - logtau/Type(2.0);
    } 
    else if(prior.name == "half_normal") {
      // HALF NORMAL prior
      // prior.par1 = mean of dnorm; always 0 because this is half normal
      // prior.par2 = SD of dnorm
      // DERIVATION
      // using log(sigma) as the variable to parameterize, then theta = exp(log(sigma))
      // so the first derivative of theta is exp(log(sigma)) = sigma
      // to get the log density: log(2) + dnorm(sigma, shape, scale, log=true) + log(sigma)
      // we can drop the constant to get the below formulation:
        penalty = dnorm(exp(log_sigma), prior.par1, prior.par2, true) + log_sigma;
        
    }
    else if(prior.name == "loggamma") {
      // GAMMA PRIOR
      // prior.par1 = shape
      // prior.par2 = scale
      // DERIVATION
      // to get from tau to log(sigma): tau = 1/(sigma^2), so logtau = -2log(sigma)
      // To get the Jacobian, the first derivative of f(theta) is:
        // dtau/dtheta = dtau/d(-2log(sigma)) = -2exp(-2log(sigma))
        // absolute value -> 2exp(-2log(sigma))
        // Then we need to solve log(density(theta)) = log(density(tau)) + log(|J|)
        // = log(density(-2log(sigma))) = log(density(-2)) + log(density(log(sigma)))
        // Solving the tau side of the equation:
          // log(density(-2)) + log(density(log(sigma))) = log(density(tau)) + log(2) - 2log(sigma)
          // Dropping the constants: dgamma(logtau, shape, scale, log=T) - 2log(sigma)
          penalty = dgamma(tau, prior.par1, prior.par2, true) - 2*log_sigma;
          
    }
    
    return penalty;
  }