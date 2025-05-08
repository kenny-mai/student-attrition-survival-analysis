# Load packages
library("tidyverse")
library("survival")
library("CoxR2")
library("ggplot2")

# Load data
baseline_data <- read_csv("baseline_data.csv")
exposure_data <- read_csv("exposure_data.csv")

# Exploration if we removed Queens schools, which normally have higher scholar retention.
# baseline_data <- baseline_data %>%filter(borough != "Queens")

# Create time dependent datasets
merged_data <- tmerge(baseline_data, baseline_data, id = sa_scholar_id,
                      withdrawn = event(last_day_recorded, attrited))
merged_data <- tmerge(merged_data, exposure_data, id = sa_scholar_id,
                      tardy_days = tdc(time_interval, tardy_days))
merged_data <- tmerge(merged_data, exposure_data, id = sa_scholar_id,
                      absent_days = tdc(time_interval, absent_days))
merged_data <- tmerge(merged_data, exposure_data, id = sa_scholar_id,
                      total_rep = tdc(time_interval, total_rep))
merged_data <- tmerge(merged_data, exposure_data, id = sa_scholar_id,
                      total_sus = tdc(time_interval, total_sus))

# Log normalizing the variables 
final_data <- merged_data %>% 
  rename(reprimands = total_rep,
         suspensions = total_sus) %>% 
  mutate(sped_status = ifelse(is.na(less_than_20)==TRUE, 0, 1),
         commute_time = log(commute_time+1),
         tardy_days = log(tardy_days+1),
         absent_days = log(absent_days+1),
         reprimands = log(reprimands+1),
         suspensions = log(suspensions+1)) %>% 
  select(sa_scholar_id,
         tstart,
         tstop,
         withdrawn,
         tardy_days,
         absent_days,
         reprimands,
         suspensions,
         commute_time,
         late_enroll,
         gender_female,
         ell_status,
         frpl_status,
         sped_status
  )

# Quick sanity checks
check <- final_data %>% 
  group_by(sa_scholar_id) %>% 
  summarize(total_sus = sum(suspensions)+sum(reprimands)) %>% 
  mutate(ever_sus = ifelse((total_sus == 0 | is.na(total_sus)==TRUE), 0, 1)) 
sum(check$ever_sus)

# Fit time-dependent Cox Proportional Hazards model 
mod_1 <- coxph(Surv(tstart, tstop, withdrawn) 
               ~ tardy_days 
               + absent_days 
               + reprimands 
               + suspensions 
               + commute_time 
               + late_enroll 
               + gender_female 
               + ell_status 
               + frpl_status 
               + sped_status
               , data = final_data
               , cluster = sa_scholar_id)

# Check summary and pseudo-R2 for goodness of fit. 
summary(mod_1)
coxr2(mod_1)


