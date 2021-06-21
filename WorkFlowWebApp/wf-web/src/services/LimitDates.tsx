import { MinMaxDatesDto } from '../model/DataModel'
import axios, { AxiosInstance } from 'axios';
import MomentTimezone from 'moment-timezone';
import { ServiceConfig } from './api.config'
import { Moment } from 'moment-timezone';
import { Observable } from 'rxjs';
var NDJsonRxJS = require('ndjson-rxjs');




export interface LimitDatesService {
    getDatesOfImages(): Promise<ReadableStream>;
    getDatesOfImagesWithURL(url?: string): Promise<ReadableStream>;
    getLimits(callback: (lim?: MinMaxDatesDto) => void): void;
    getLimitsByDate(min: Moment, max: Moment, intervalType: string, callback: (lim?: MinMaxDatesDto[] | null) => void): void;
}

export default class LimitDatesServiceImpl implements LimitDatesService {

    axiosInstance: AxiosInstance;
    limitURL: string;
    countAllImagesURL: string;
    baseUrlToGetDates: string;

    protected buildURLToGetDates(min: Moment, max: Moment, intervalType: string): string {
        console.log('  buildURLToGetDates  : ' + min.format('YYYY-MM-DD HH:mm:ss +01') + ' maxDate ' + max.format('YYYY-MM-DD HH:mm:ss +01'));

        var params: { [key: string]: string } = {
            intervalType: intervalType,
            firstDate: encodeURIComponent(min.format('YYYY-MM-DD HH:mm:ss +01')),
            lastDate: encodeURIComponent(max.format('YYYY-MM-DD HH:mm:ss +01'))
        };
        var queryString = Object.keys(params).map(key => params[key]).join('/');
        return this.baseUrlToGetDates + queryString;
    }
    constructor() {
        this.axiosInstance = axios.create(ServiceConfig);
        this.limitURL = 'api/gs/images/dates/limits';
        this.countAllImagesURL = '/api/gs/images/count/all';
        this.baseUrlToGetDates = '/api/gs/images/odt/dates/limits/';
    }

    async getLimitsByDate(min: Moment, max: Moment, intervalType: string, callback: (lim?: MinMaxDatesDto[] | null) => void) {
        console.log('  call getLimitsByDate : ' + min.toString() + ' maxDate ' + max.toString() + ' / ' + intervalType);
        const urlToGetDates = this.buildURLToGetDates(min, max, intervalType);
        const [dateLimits] = await Promise.all([
            this.axiosInstance.get(urlToGetDates),
        ]);
        var retValue: MinMaxDatesDto[] = new Array(dateLimits.data.length);
        for (var index = 0; index < dateLimits.data.length; index++) {
            const minDate = MomentTimezone(dateLimits.data[index].minDate, 'YYYY-MM-DD HH:mm:ss');
            const maxDate = MomentTimezone(dateLimits.data[index].maxDate, 'YYYY-MM-DD HH:mm:ss');
            retValue[index] = {
                minDate: minDate,
                maxDate: maxDate,
                countNumber: 0,
                intervallType: ''
            };
            console.log('  getLimitsByDate ' + index + ' : ' + minDate.toString() + ' maxDate ' + maxDate.toString() + ' / ' + dateLimits.data[index].maxDate + ' -  ' + dateLimits.data[index].minDate);

        }

        callback(retValue);
    }

    async getDatesOfImages(): Promise<ReadableStream> {
        const urlToGetDates = '/api/gs/images/dates/limits/allyears';
        return this.getDatesOfImagesWithURL(urlToGetDates);
    }

    async getDatesOfImagesWithURL(urlToGetDates?: string): Promise<ReadableStream> {
        const url = urlToGetDates == null ? '/api/gs/images/dates/limits/allyears' : urlToGetDates
        const reponse: Observable<string> = await NDJsonRxJS.stream(url, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/stream+json',
                'Accept': 'application/stream+json'
            }
        });

        return new ReadableStream({
            start(controller) {
                pump();
                async function pump() {
                    reponse.subscribe({
                        next(v: string): void {
                            controller.enqueue(v);
                        },
                        complete(): void {
                            controller.close();
                        }

                    })
                }
            }
        });
    }


    async getLimits(callback: (lim?: MinMaxDatesDto) => void) {
        const [dateLimits, totalNb] = await Promise.all([
            this.axiosInstance.get(this.limitURL),
            this.axiosInstance.get(this.countAllImagesURL)
        ]);
        const minDate = MomentTimezone(dateLimits.data.minDate, 'YYYY-MM-DD HH:mm:ss');
        const maxDate = MomentTimezone(dateLimits.data.maxDate, 'YYYY-MM-DD HH:mm:ss');
        const retValue: MinMaxDatesDto = {
            minDate: minDate,
            maxDate: maxDate,
            countNumber: totalNb.data,
            intervallType: ''
        };
        callback(retValue);
    }

}
