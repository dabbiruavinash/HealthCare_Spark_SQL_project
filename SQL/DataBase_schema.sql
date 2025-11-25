-- Patients table
CREATE TABLE patients (
    patient_id NUMBER PRIMARY KEY,
    first_name VARCHAR2(50),
    last_name VARCHAR2(50),
    date_of_birth DATE,
    gender VARCHAR2(10),
    phone_number VARCHAR2(15),
    email VARCHAR2(100),
    address VARCHAR2(200),
    emergency_contact VARCHAR2(100),
    blood_type VARCHAR2(5),
    created_date DATE DEFAULT SYSDATE
);

-- Doctors table
CREATE TABLE doctors (
    doctor_id NUMBER PRIMARY KEY,
    first_name VARCHAR2(50),
    last_name VARCHAR2(50),
    specialization VARCHAR2(100),
    license_number VARCHAR2(50),
    phone_number VARCHAR2(15),
    email VARCHAR2(100),
    hire_date DATE,
    salary NUMBER(10,2),
    department VARCHAR2(100)
);

-- Departments table
CREATE TABLE departments (
    department_id NUMBER PRIMARY KEY,
    department_name VARCHAR2(100),
    head_doctor_id NUMBER,
    budget NUMBER(15,2),
    location VARCHAR2(100),
    FOREIGN KEY (head_doctor_id) REFERENCES doctors(doctor_id)
);

-- Appointments table
CREATE TABLE appointments (
    appointment_id NUMBER PRIMARY KEY,
    patient_id NUMBER,
    doctor_id NUMBER,
    appointment_date TIMESTAMP,
    status VARCHAR2(20),
    reason VARCHAR2(200),
    duration_minutes NUMBER,
    room_number VARCHAR2(20),
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
    FOREIGN KEY (doctor_id) REFERENCES doctors(doctor_id)
);

-- Medical_records table
CREATE TABLE medical_records (
    record_id NUMBER PRIMARY KEY,
    patient_id NUMBER,
    doctor_id NUMBER,
    visit_date DATE,
    diagnosis VARCHAR2(200),
    symptoms VARCHAR2(500),
    treatment_plan VARCHAR2(500),
    follow_up_required CHAR(1),
    follow_up_date DATE,
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
    FOREIGN KEY (doctor_id) REFERENCES doctors(doctor_id)
);

-- Prescriptions table
CREATE TABLE prescriptions (
    prescription_id NUMBER PRIMARY KEY,
    record_id NUMBER,
    medication_name VARCHAR2(100),
    dosage VARCHAR2(50),
    frequency VARCHAR2(50),
    start_date DATE,
    end_date DATE,
    refills_remaining NUMBER,
    FOREIGN KEY (record_id) REFERENCES medical_records(record_id)
);

-- Lab_tests table
CREATE TABLE lab_tests (
    test_id NUMBER PRIMARY KEY,
    record_id NUMBER,
    test_name VARCHAR2(100),
    test_date DATE,
    result_value VARCHAR2(100),
    normal_range VARCHAR2(100),
    units VARCHAR2(20),
    lab_technician VARCHAR2(100),
    status VARCHAR2(20),
    FOREIGN KEY (record_id) REFERENCES medical_records(record_id)
);

-- Billing table
CREATE TABLE billing (
    bill_id NUMBER PRIMARY KEY,
    patient_id NUMBER,
    appointment_id NUMBER,
    total_amount NUMBER(10,2),
    insurance_covered NUMBER(10,2),
    patient_balance NUMBER(10,2),
    bill_date DATE,
    due_date DATE,
    status VARCHAR2(20),
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
    FOREIGN KEY (appointment_id) REFERENCES appointments(appointment_id)
);

-- Insurance table
CREATE TABLE insurance (
    insurance_id NUMBER PRIMARY KEY,
    patient_id NUMBER,
    provider_name VARCHAR2(100),
    policy_number VARCHAR2(50),
    coverage_type VARCHAR2(50),
    deductible NUMBER(10,2),
    copay NUMBER(10,2),
    effective_date DATE,
    expiration_date DATE,
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
);
