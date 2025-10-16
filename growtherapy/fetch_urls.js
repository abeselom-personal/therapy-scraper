import fs from 'fs'
import fetch from 'node-fetch'

const states = [
    'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia',
    'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts',
    'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey',
    'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island',
    'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'
]

const BASE_URL = 'https://growtherapy.com/api/provider-search?shouldUseSrpDescriptions=false&limit=5000&cacheControl=no-cache&fetchPolicy=cache-first&isEnhancedPagination=true&fetchPageCount=false&isLowNoSupplyState=false&isSpecialtiesFilterWithAnd=false&isExactMatchForFilters=false&name=&sortAlgorithmVersion=provider_ranking_algo_v13a&timeZone=UTC'

const sleep = ms => new Promise(r => setTimeout(r, ms))

const fetchAllStates = async () => {
    const allProviders = []

    for (const state of states) {
        console.log(`[DEBUG] Fetching state: ${state}`)
        try {
            const url = `${BASE_URL}&state=${encodeURIComponent(state)}`
            const res = await fetch(url)
            const json = await res.json()
            const providers = json.marketplaceData?.paginatedMarketplaceProviders?.providers || []
            console.log(`[DEBUG] Providers found: ${providers.length}`)

            const formatted = providers.map((p, i) => ({
                SrNO: allProviders.length + i + 1,
                Url: `https://growtherapy.com/provider/${p.id}-${p.name.toLowerCase().replace(/\s+/g, '-')}`,
                Name: p.name,
                Profession: p.license || '',
                ClinicName: '',
                Bio: p.description || '',
                AdditionalFocusAreas: p.specialties?.join(', ') || '',
                TreatmentApproaches: '',
                AppointmentTypes: p.availabilitySettingsByState || '',
                Communities: '',
                AgeGroups: '',
                Languages: '',
                Highlights: p.topProviderFeedbackTags?.join(', ') || '',
                Gender: '',
                Pronouns: p.pronouns || '',
                RaceEthnicity: '',
                Licenses: p.license || '',
                Locations: p.physicalAddress || `${p.virtualCity || ''}, ${p.virtualState || ''}`,
                Education: '',
                Faiths: '',
                MinSessionPrice: p.price || '',
                MaxSessionPrice: p.price || '',
                PayOutOfPocketStatus: '',
                IndividualServiceRates: '',
                GeneralPaymentOptions: '',
                BookingSummary: p.nextAvailableAppointment || '',
                BookingUrl: `https://growtherapy.com/provider/${p.id}-${p.name.toLowerCase().replace(/\s+/g, '-')}`,
                ListedInStates: p.state || state,
                States: p.state || state,
                ListedInWebsites: 'growtherapy',
                Urls: `https://growtherapy.com/provider/${p.id}-${p.name.toLowerCase().replace(/\s+/g, '-')}`,
                ConnectFacebook: '',
                ConnectInstagram: '',
                ConnectLinkedIn: '',
                ConnectTwitter: '',
                ConnectWebsite: '',
                MainSpecialties: p.topSpecialties?.join(', ') || '',
                AcceptedIPs: '',
                AppointmentsIn7Days: p.nextAvailableAppointment ? 'Yes' : 'No'
            }))

            allProviders.push(...formatted)
            await sleep(1000)
        } catch (err) {
            console.error(`[ERROR] ${state}:`, err.message)
        }
    }

    // Save JSON
    fs.writeFileSync('./growtherapy_all_states.json', JSON.stringify(allProviders, null, 2))

    // Save CSV
    const csv = [
        Object.keys(allProviders[0]).join('\t'),
        ...allProviders.map(p => Object.values(p).join('\t'))
    ].join('\n')
    fs.writeFileSync('./growtherapy_all_states.csv', csv)

    console.log(`[DEBUG] Saved ${allProviders.length} providers ✅`)
}

fetchAllStates()
