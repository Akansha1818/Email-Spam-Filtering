export const SAMPLES = [
  {
    kind: "spam" as const,
    title: "Prize winner",
    sender: "claims@lucky-winners.biz",
    subject: "CONGRATULATIONS!!! You have WON $1,000,000",
    body: "Dear winner, your email was selected in our international lottery. To claim your $1,000,000 cash prize click here http://claim-prize-now.biz and send your bank details today. Offer expires in 24 hours! Act now!!!",
  },
  {
    kind: "spam" as const,
    title: "Cheap meds",
    sender: "deals@pharma-discount.net",
    subject: "Save 80% on medications - no prescription needed",
    body: "Best prices on viagra, cialis and more. 100% satisfaction guaranteed. Order online now at www.cheap-meds-online.com and get free shipping worldwide. Unsubscribe here.",
  },
  {
    kind: "spam" as const,
    title: "Investment tip",
    sender: "alert@stock-insider.info",
    subject: "This stock will explode 500% next week",
    body: "Hot investment opportunity! Our analysts found a small company about to announce huge news. Buy now before the price jumps. Limited time - click the link to get the free report and double your money.",
  },
  {
    kind: "ham" as const,
    title: "Team meeting",
    sender: "priya@company.com",
    subject: "Agenda for tomorrow's project meeting",
    body: "Hi team, please review the attached quarterly report before our meeting tomorrow at 10 am. We will discuss the schedule for the next phase and the budget. Let me know if you have questions. Thanks, Priya",
  },
  {
    kind: "ham" as const,
    title: "Gas contract",
    sender: "john.arnold@enron.com",
    subject: "Re: gas contract volumes for march",
    body: "Vince, attached are the revised volumes for the march gas contract. Please check the pricing with the trading desk and send me your comments by friday so we can finalise the deal with the pipeline.",
  },
  {
    kind: "ham" as const,
    title: "Lunch plans",
    sender: "rahul@gmail.com",
    subject: "lunch on friday?",
    body: "Hey, are you free for lunch on friday? I was thinking we could try the new place near the office around 1 pm. Let me know what works for you.",
  },
];
