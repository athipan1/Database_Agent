หน้าที่หลักของ Database_Agent

1️⃣ เก็บทุกการตัดสินใจ (Decision Log)
	•	buy / sell / hold
	•	มาจาก agent ไหน
	•	confidence เท่าไหร่
	•	correlation_id เดียวกับทั้ง flow

2️⃣ เก็บผลลัพธ์จริง (Outcome)
	•	ราคาหลังจาก t+1, t+7, t+30
	•	กำไร / ขาดทุน
	•	drawdown

3️⃣ เป็นแหล่งข้อมูลให้ LearningAgent
	•	ดูย้อนหลังว่า ใครทำนายแม่น
	•	ใช้คำนวณ reward / penalty
	•	ปรับ weight ของ agent อื่น

4️⃣ Audit / Trace ได้ทั้งระบบ
	•	1 correlation_id → ไล่ได้ทั้ง chain
	•	Debug ง่าย
	•	อธิบายการตัดสินใจได้ (Explainability)