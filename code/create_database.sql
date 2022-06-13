/*  Drop tables if they already exists to avoid conflicts  */

DROP TABLE IF EXISTS vaccine, manufacturer, vaccinebatch, hospitalclinic, delivery, staffmembers, shifts, vaccinationevents, patients, vaccinepatients, symptoms, diagnosis CASCADE;

/* --------- queries to add primary keys ------------ */

ALTER TABLE vaccine ADD PRIMARY KEY(vaccineid);   

ALTER TABLE manufacturer ADD PRIMARY KEY(manufid); 

ALTER TABLE vaccinebatch ADD PRIMARY KEY(batchid); 

ALTER TABLE vaccinationevents ADD PRIMARY KEY(date, hospitalname); 

ALTER TABLE patients ADD PRIMARY KEY(ssno); 

ALTER TABLE shifts ADD PRIMARY KEY(weekday, ssno);

ALTER TABLE symptoms ADD PRIMARY KEY (name);

ALTER TABLE hospitalclinic ADD PRIMARY KEY(name); 

ALTER TABLE staffmembers ADD PRIMARY KEY(ssno); 

ALTER TABLE vaccinepatients ADD PRIMARY KEY(ssno, date);

/* --------- queries to add foreign keys ------------ */

ALTER TABLE manufacturer 
       ADD FOREIGN KEY (vaccineid) REFERENCES vaccine(vaccineid);
        
ALTER TABLE vaccinebatch 
       ADD FOREIGN KEY (manufid) REFERENCES manufacturer(manufid);
        
ALTER TABLE vaccinebatch 
       ADD FOREIGN KEY (hospitalname) REFERENCES hospitalclinic(name);
        
ALTER TABLE vaccinationevents 
       ADD FOREIGN KEY (batchid) REFERENCES vaccinebatch(batchid);
            
ALTER TABLE delivery 
       ADD FOREIGN KEY (batchid) REFERENCES vaccinebatch(batchid);      
    
ALTER TABLE staffmembers 
       ADD FOREIGN KEY (hospitalname) REFERENCES hospitalclinic(name);
        
ALTER TABLE vaccinepatients 
       ADD FOREIGN KEY (hospitalname) REFERENCES hospitalclinic(name);
        
ALTER TABLE shifts 
       ADD FOREIGN KEY (hospitalname) REFERENCES hospitalclinic(name);
        