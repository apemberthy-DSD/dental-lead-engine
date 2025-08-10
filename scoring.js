function detectFeatures(text=''){
  const t = text.toLowerCase();

  const has_online_scheduling = /book online|schedule online|book appointment|book now|zocdoc|localmed|doctible|nexhealth|jane app|setmore|calendly/.test(t);
  const has_patient_portal   = /patient portal|patient login|my chart|patient account/.test(t);
  const has_text_reminders   = /text reminders|sms reminder|text notification|weave|revenuewell|solutionreach|yapi/.test(t);
  const has_digital_forms    = /online forms|digital forms|paperless|fill out.*online/.test(t);
  const has_online_payments  = /pay online|online payment|carecredit|care credit|financing/.test(t);
  const has_virtual_consults = /virtual consultation|video consultation|teledentistry|telehealth/.test(t);
  const has_advanced_imaging = /3d imaging|cbct|cerec|same day crown|digital impression|itero|laser/.test(t);

  const technologies = [];
  if (has_online_scheduling) technologies.push('onlineScheduling');
  if (has_patient_portal) technologies.push('patientPortal');
  if (has_text_reminders) technologies.push('textReminders');
  if (has_digital_forms) technologies.push('digitalForms');
  if (has_online_payments) technologies.push('onlinePayments');
  if (has_virtual_consults) technologies.push('virtualConsults');
  if (has_advanced_imaging) technologies.push('advancedDentalTech');

  return {
    has_online_scheduling,
    has_patient_portal,
    has_text_reminders,
    has_digital_forms,
    has_online_payments,
    has_virtual_consults,
    has_advanced_imaging,
    technologies
  };
}

function computeTechScore(f){
  let score = 0;
  if (f.has_online_scheduling) score += 25;
  if (f.has_patient_portal)   score += 20;
  if (f.has_text_reminders)   score += 15;
  if (f.has_digital_forms)    score += 10;
  if (f.has_online_payments)  score += 10;
  if (f.has_virtual_consults) score += 10;
  if (f.has_advanced_imaging) score += 10;
  return Math.min(100, score);
}

function subscores({ techScore=0, rating=0, reviews=0, hasBooking=false, specialtyBoost=0 }){
  const sTech = techScore;                                   // 40%
  const sBooking = hasBooking ? 100 : 0;                     // 15%
  const sRating = Math.max(0, Math.min(100, ((rating-3.5)/1.5)*100)); // 15%
  const sReviews = reviews>=200?100:reviews>=100?80:reviews>=50?60:reviews>=25?40:reviews>=10?20:0; // 10%
  const sSpecial = Math.max(0, Math.min(100, specialtyBoost)); // 20% (LLM boost)
  const final = Math.round(0.40*sTech + 0.15*sBooking + 0.15*sRating + 0.10*sReviews + 0.20*sSpecial);
  return { sTech, sBooking, sRating, sReviews, sSpecial, final };
}

function tierFromScore(score){
  if(score>=80) return {tier:'PLATINUM',qual:'HOT'};
  if(score>=60) return {tier:'GOLD',qual:'HOT'};
  if(score>=40) return {tier:'SILVER',qual:'WARM'};
  if(score>=20) return {tier:'BRONZE',qual:'COOL'};
  return {tier:'BASIC',qual:'COLD'};
}

module.exports = { detectFeatures, computeTechScore, subscores, tierFromScore };
