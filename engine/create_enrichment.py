"""Create enrichment JSON from collected premium data sources."""
import json

# PitchBook data (from our query)
pitchbook_data = [
    {
        "content": "Gymshark (legal name: Gymshark Limited) is a Solihull, England, United Kingdom-based company founded in 2012. Manufacturer of fitness apparel and accessories intended to serve athletes, gym-goers, and sports enthusiasts. The company designs and markets jackets, tracksuits, bags, and customized apparel for both men and women. Gymshark operates in the Consumer Products and Services (B2C) sector within the Apparel and Accessories group (industry code: Clothing). The company is currently Generating Revenue with Privately Held (backing) and Private Equity-Backed (as of 09/19/2023). Gymshark employs 1000 people (as of 10/24/2025) and reported 577.42M in revenue for TTM 4Q2024. The company's website is www.gymshark.com. Gymshark is headquartered at GSHQ Blythe Valley Business Park, 3 Central Boulevard, Solihull, England, B90 8AB, United Kingdom.",
        "source_url": "https://cashmere.io/v/A4LMR4",
        "source_name": "PitchBook",
        "title": "Gymshark Company Profile",
        "publisher": "PitchBook Essentials",
        "published_at": "2026-02-06T00:00:00",
        "score": 20.21
    },
    {
        "content": "Gymshark has raised 251.11M million USD in total funding. The most recent financing was a Debt - General of 251.11M closed on 09/19/2023. In 09/16/2020, Gymshark raised in a PE Growth/Expansion round and a post-money valuation of 1,312.61M (Estimated). Revenue at time of deal was 328.60M. In 10/12/2021, Gymshark raised in an IPO round. Revenue at time of deal was 592.95M. In 09/19/2023, Gymshark raised 251.11M in a Debt - General round. Revenue at time of deal was 488.43M. Current Investors: General Atlantic (www.generalatlantic.com) has been an investor in Gymshark since 09/16/2020. Status: Active. Holding: Minority. Similar Companies: Outdoor Voices, Myles Athletic, Eddie Bauer, Reebok, prAna, Ten Thousand, J.Crew Group, Lands End, Ann Taylor, Fabletics.",
        "source_url": "https://cashmere.io/v/yujKsF",
        "source_name": "PitchBook",
        "title": "Gymshark Funding & Investors",
        "publisher": "PitchBook Essentials",
        "published_at": "2026-02-06T00:00:00",
        "score": 16.39
    },
    {
        "content": "Gymshark Group (legal name: Gymshark Group Limited) is a Solihull, England, United Kingdom-based company founded in 2020. The company primarily operates in the Holding Companies industry. Gymshark Group employs 881 people (as of 12/31/2024) and reported 764.66M in revenue for TTM 4Q2024. Gymshark Group is headquartered at Gshq Blythe Valley Park 3 Central Boulevard, Solihull, England, B90 8AB, United Kingdom.",
        "source_url": "https://cashmere.io/v/z2Z4uS",
        "source_name": "PitchBook",
        "title": "Gymshark Group Company Profile",
        "publisher": "PitchBook Essentials",
        "published_at": "2026-02-06T00:00:00",
        "score": 14.47
    },
    {
        "content": "General Atlantic — Investor Profile. Primary Investor Type: Growth/Expansion. AUM: 123,000M USD. Year Founded: 1980. Website: www.generalatlantic.com. HQ Location: New York, NY. Investment Professional Count: 349 (as of 01/06/2026).",
        "source_url": "https://cashmere.io/v/72YUlc",
        "source_name": "PitchBook",
        "title": "General Atlantic Investor Profile",
        "publisher": "PitchBook",
        "published_at": "2026-03-03T00:00:00",
        "score": 15.57
    }
]

# CB Insights data
cbinsights_data = [
    {
        "content": "We Analyzed 22 Of The Biggest Direct-To-Consumer Success Stories. Gymshark: Using influencer marketing to grow a cult-like following among gym-goers. Ben Francis founded Gymshark in 2012 in the UK when he was just 19. Unhappy with existing sportswear choices, he wanted to build a clothing brand that offered affordable and fashionable products to gym-goers. The company initially contacted high-profile accounts such as bodybuilders Lex Griffin, Chris Lavado, and Nikki Blackketter. In exchange for free apparel, these influencers were supposed to wear and promote the products. The strategy proved highly successful. Gymshark now sponsors 18 influencers, including Irish professional boxer Katie Taylor and ultra-marathon sea swimmer Ross Edgley. In 2020, US fund manager General Atlantic invested over $250M into Gymshark, valuing the company at $1.3B. The funds will be used to expand the business into new markets, including North America and Asia.",
        "source_url": "https://cashmere.io/v/AwOcAm",
        "source_name": "CB Insights",
        "title": "22 Biggest DTC Success Stories Analysis",
        "publisher": "CB Insights",
        "published_at": "2020-12-08T09:00:12",
        "score": 19.26
    },
    {
        "content": "Private Company eCommerce Exit Valuation Multiples: Median 2.0x and Average 5.8x. Since 2007, the range of eCommerce exit valuation multiples has varied widely from 0.005x to 75x. Over the period, the average price/sales ratio has stood at 5.8x and the median at 2.0x.",
        "source_url": "https://cashmere.io/v/cpkbHu",
        "source_name": "CB Insights",
        "title": "eCommerce Exit Valuation Multiples",
        "publisher": "CB Insights",
        "published_at": "2013-06-27T00:00:00",
        "score": 19.36
    }
]

# Statista data
statista_data = [
    {
        "content": "Revenue of the leading sportswear companies worldwide in 2023 (in billions of USD): Nike ~51.2B, Adidas ~23.4B, Puma ~9.3B, Under Armour ~5.7B, Lululemon ~9.6B, VF Corporation ~11.6B. Sources: Nike Inc., adidas AG, Puma, Under Armour, lululemon athletica, VF Corporation.",
        "source_url": "https://cashmere.io/v/de8ivX",
        "source_name": "Statista",
        "title": "Revenue of Leading Sportswear Companies Worldwide 2023",
        "publisher": "Statista",
        "published_at": "2025-07-10T20:23:53+00:00",
        "score": 41.33
    },
    {
        "content": "Global athletic apparel market share by company in 2015: Adidas 11.6%, Nike 10.8%, VF Corp, Under Armour, Gildan. Nike and Adidas are by some margin the leading companies in the sportswear industry worldwide. Nike dominates the U.S. athletic footwear market with over half of the market share.",
        "source_url": "https://cashmere.io/v/akFlyo",
        "source_name": "Statista",
        "title": "Global Athletic Apparel Market Share 2015",
        "publisher": "Statista",
        "published_at": "2025-11-26T18:55:16+00:00",
        "score": 27.76
    },
    {
        "content": "Sports apparel market share in the United States in 2018 by company: Nike 18.3%, Adidas 6%. Nike is a key player within the U.S. sports apparel market.",
        "source_url": "https://cashmere.io/v/lcn3Ly",
        "source_name": "Statista",
        "title": "Sports Apparel Market Share US 2018",
        "publisher": "Statista",
        "published_at": "2025-11-26T20:00:08+00:00",
        "score": 34.23
    },
    {
        "content": "Revenue of the fitness market in the United Kingdom from 2019 to 2023 (in billion euros): 2023 = 5.5 billion euros. The revenue fell significantly between 2019 and 2020 due to COVID-19 but has since shown significant recovery. Sources: EuropeActive, Deloitte.",
        "source_url": "https://cashmere.io/v/CJmXJr",
        "source_name": "Statista",
        "title": "UK Fitness Market Revenue 2023",
        "publisher": "Statista",
        "published_at": "2025-11-26T18:58:13+00:00",
        "score": 16.55
    },
    {
        "content": "Forecasted retail value of the domestic athletic leisure apparel (athleisure) market in Japan from 2017 to 2030: In 2030, the Japanese athleisure market was forecasted to reach approximately 900 billion Japanese yen. Sources: Yano Research Institute.",
        "source_url": "https://cashmere.io/v/d31qFY",
        "source_name": "Statista",
        "title": "Japan Athleisure Market Size Forecast 2030",
        "publisher": "Statista",
        "published_at": "2025-11-26T19:58:09+00:00",
        "score": 37.79
    },
    {
        "content": "Nike was the preferred athletic and sportswear brand among almost one quarter of Canadians surveyed in 2020. Adidas and Under Armour ranked second and third. The most important purchase factors were comfort, price, and quality — just over 5% cited brand as a leading influence.",
        "source_url": "https://cashmere.io/v/bzUsHv",
        "source_name": "Statista",
        "title": "Favorite Athletic Apparel Brands Canada 2020",
        "publisher": "Statista",
        "published_at": "2025-11-26T19:54:35+00:00",
        "score": 28.07
    }
]

enrichment = {
    "pitchbook": pitchbook_data,
    "cbinsights": cbinsights_data,
    "statista": statista_data,
}

# Save for Gymshark test
import os
output_path = "/tmp/gymshark_enrichment.json"
with open(output_path, "w") as f:
    json.dump(enrichment, f, indent=2)

print(f"Enrichment file created: {output_path}")
print(f"  PitchBook: {len(pitchbook_data)} entries")
print(f"  CB Insights: {len(cbinsights_data)} entries")
print(f"  Statista: {len(statista_data)} entries")
