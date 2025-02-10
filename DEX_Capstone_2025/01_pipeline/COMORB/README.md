## Comorbidity adjustment


#### Maintainer: Haley Lescinsky

#### Overview:
The comorbidity adjustment (comorb) is based on that idea that patients who have multiple ailments are likely to pay more for an encounter because of their comorbidities. In the DEX project we mostly focus on the primary cause and without the comorb adjustment, 100% of the spending associated with an encounter would be attributed to that primary cause. However, since our desire is to accurately capture expenditure due to various health conditions, through the comorbidity adjustment some of the spending initially attributed to the primary cause is redirected to the key comorbidities of that primary cause. 

#### Details:
The comorb maps are attributable fractions quantifying how much of the spending on condition X should actually be attributable to condition Y. In order to calculate the attributable fraction (AF), we use encounter level data to quantify the relative risk of increased spending on an encounter for condition X due to comorbidity Y. Then combine that with the conditional probability that those two conditions both occur (a measure of 'how much this happens') to calculate the AF. Then, unlike other adjustments, we apply comorb on collapsed data. s