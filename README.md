# Portfolio Project 1: Data Pipeline from Wikimedia
สร้าง Data Pipeline อย่างง่ายจากข้อมูลของ Wikimedia Pageviews

## ที่มา
เป็นโครงงานฝึกฝีมือการทำ Data Pipeline ที่สามารถสร้างได้โดยไม่ต้องพึ่งพาระบบคลาวด์หรือเครื่องมือช่วยจัดการ Transformation ภายนอก

## การทำงาน Data Pipeline
- ดาวน์โหลดไฟล์ Pageview จาก Wikimedia ซึ่งจะออกมาทุก 1 ชั่วโมง
- อัปโหลดไฟล์เข้าสู่ระบบฐานข้อมูล PostgreSQL ในฐานข้อมูลที่กำหนดไว้ ซึ่งจะเป็นข้อมูลรอการแปลง (Staging Area)
- ทำการแปลง (Transform) ข้อมูลในระบบ PostgreSQL และข้อมูลที่แปลงเสร็จจะเข้าไปที่ Production Area

## เครื่องมือที่ใช้
- Data Ingestion: Python
- Database ที่ไว้รับข้อมูล: PostgreSQL
- Data Transformation: pl/pgsql
- Operating System: Ubuntu 20.04

## สถานะ
- ✅ สร้าง Minimum Viable Product
