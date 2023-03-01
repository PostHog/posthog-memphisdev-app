import fetch from 'node-fetch'
import { RetryError } from '@posthog/plugin-scaffold'

async function fetchToken(url, body, global) {
    try {
        const response = await fetch(url, {
            method: 'post',
            body: JSON.stringify(body),
            headers: { 'Content-Type': 'application/json' },
        })
        if (response.status === 401) {
            throw new Error('Unauthorized')
        }

        const data = await response.json()
        global.jwt = data.jwt
        global.refresh_token = data.jwt_refresh_token
        const now = new Date().getMilliseconds()
        global.expires_in = new Date(now + response.expires_in)
        global.refresh_token_expires_in = new Date(now + response.refresh_token_expires_in)
    } catch (err) {
        console.log(`Error authenticating with REST gateway: ${err.message}`)
        throw new Error(err.message)
    }
}

async function authenticate(config, global) {
    const url = `${config.host}/auth/authenticate`
    const body = {
        username: config.user,
        connection_token: config.token,
        token_expiry_in_minutes: 60,
        refresh_token_expiry_in_minutes: 10000092,
    }
    await fetchToken(url, body, global)
    console.log('Authentication success')
}

async function refreshToken(config, global) {
    const url = `${config.host}/auth/refreshToken`
    const body = {
        jwt_refresh_token: global.refresh_token,
        token_expiry_in_minutes: 60,
        refresh_token_expiry_in_minutes: 10000092,
    }
    try {
        await fetchToken(url, body, global)
        console.log('Token refreshed')
    } catch (err) {
        if (err.message.includes('Unauthorized')) {
            await authenticate(config, global)
        } else {
            throw new Error(err.message)
        }
    }
}
export async function setupPlugin({ config, global }) {
    if (!config.host) {
        throw new Error('Host address missing!')
    } else if (config.host.slice(-1) === '/') {
        config.host = config.host.substring(0, config.host.length - 1)
    }
    if (!config.user) {
        throw new Error('Username missing!')
    }
    if (!config.token) {
        throw new Error('Connection token missing!')
    }
    if (!config.station) {
        throw new Error('Station name missing!')
    }
    console.debug('Configurations: ', { ...config, token: '' })
    await authenticate(config, global)
    global.eventsToIgnore = new Set(
        config.eventsToIgnore ? config.eventsToIgnore.split(',').map((event) => event.trim()) : null
    )
}

async function sendEventsToMemphisStation(events, { config, global }) {
    const url = `${config.host}/stations/${config.station}/produce/single`
    try {
        let expired = global.expires_in.getTime() < new Date().getTime()
        if (expired) {
            expired = global.refresh_token_expires_in.getTime() < new Date().getTime()
            if (expired) await authenticate(config, global)
            else await refreshToken(config, global)
        }
        const response = await fetch(url, {
            method: 'post',
            body: JSON.stringify(events),
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${global.jwt}`,
            },
        })
        const data = await response.json()
        const eventCount = `${events.length} event${events.length === 1 ? '' : 's'}`
        if (data.success) {
            console.log(`Published ${eventCount} to Memphis`)
        } else {
            console.log(`Error publishing ${eventCount}`, data.error)
        }
    } catch (err) {
        console.log(`Error: ${err.message}`)
        throw new RetryError(err.message)
    }
}
export async function exportEvents(events, meta) {
    const eventsToExport = events.filter((event) => !meta.global.eventsToIgnore.has(event.event))
    if (eventsToExport.length > 0) {
        await sendEventsToMemphisStation(eventsToExport, meta)
    }
}
