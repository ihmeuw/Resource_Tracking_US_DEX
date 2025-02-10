
## C2E (Claims to Encounter)

Dependencies: C2E follows Causemap (and Zipfix which is only for Kythera).
Note that if additional columns have been added in data processing which are necessary for C2E, those variables must be updated in Causemap.

Purpose: Aggregate claim level (or line level) data based on beneficiary ID and date of care, assign an encounter ID, and sum spending. In other words, we group claims to the encounter level (ie. many claims: 1 encounter) which is our modeling unit for estimating spending and utilization. 
We define an encounter as all claims with the same
- Type of care
- Beneficiary
- Date of admission (for inpatient and nursing facility encounters) or date of service event with a provider (for ambulatory, emergency department, and home health encounters.)

