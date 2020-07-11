import { MinMaxDatesDto } from '../model/MinMaxDatesDto'
import axios, { AxiosInstance } from 'axios';
import MomentTimezone from 'moment-timezone';
import { ServiceConfig } from './api.config'
import { Moment} from 'moment-timezone';


export interface LimitDatesService {
	getLimits(callback: (lim?: MinMaxDatesDto) => void): void;
	getLimitsByDate(min: Moment, max:Moment, intervalType:string, callback: (lim?: MinMaxDatesDto[] | null) => void): void;
}

export default class LimitDatesServiceImpl implements LimitDatesService {

	axiosInstance: AxiosInstance;
	limitURL: string;
	countAllImagesURL: string;
	baseUrlToGetDates: string;

	protected buildURLToGetDates(min: Moment, max:Moment, intervalType: string):string {
		console.log('  buildURLToGetDates  : '+  min.format('YYYY-MM-DD HH:mm:ss +01') + ' maxDate ' + max.format('YYYY-MM-DD HH:mm:ss +01'));

		var params : { [key:string]:string }= {
			intervalType: intervalType,
			firstDate: encodeURIComponent(min.format('YYYY-MM-DD HH:mm:ss +01')),
			lastDate: encodeURIComponent(max.format('YYYY-MM-DD HH:mm:ss +01'))
		};
		var queryString = Object.keys(params).map(key => params[key]).join('/');
		return this.baseUrlToGetDates + queryString ;
	}
	constructor() {
		this.axiosInstance = axios.create(ServiceConfig);
		this.limitURL = 'api/gs/images/dates/limits';
		this.countAllImagesURL = '/api/gs/images/count/all';
		this.baseUrlToGetDates = '/api/gs/images/odt/dates/limits/';
	}

	async getLimitsByDate(min: Moment, max:Moment, intervalType:string, callback: (lim?: MinMaxDatesDto[]| null) => void) {
		console.log('  call getLimitsByDate : '+  min.toString() + ' maxDate ' + max.toString() + ' / ' + intervalType);
		const urlToGetDates = this.buildURLToGetDates(min, max,intervalType);
		const [dateLimits] = await Promise.all([
			this.axiosInstance.get(urlToGetDates),
		]);
		var retValue: MinMaxDatesDto[] = new Array( dateLimits.data.length);
		for(var index=0 ; index< dateLimits.data.length ;index++) 
		{  
			const minDate = MomentTimezone(dateLimits.data[index].minDate, 'YYYY-MM-DD HH:mm:ss');
			const maxDate = MomentTimezone(dateLimits.data[index].maxDate, 'YYYY-MM-DD HH:mm:ss');
			retValue[index] = { "minDate": minDate, "maxDate": maxDate, "nbOfImages": 0 };
			console.log('  getLimitsByDate ' + index + ' : '+  minDate.toString() + ' maxDate ' + maxDate.toString() + ' / ' + dateLimits.data[index].maxDate + ' -  ' + dateLimits.data[index].minDate);

		} 
		
		callback(retValue);
	}


	async getLimits(callback: (lim?: MinMaxDatesDto) => void) {
		const [dateLimits, totalNb] = await Promise.all([
			this.axiosInstance.get(this.limitURL),
			this.axiosInstance.get(this.countAllImagesURL)
		]);
		const minDate = MomentTimezone(dateLimits.data.minDate, 'YYYY-MM-DD HH:mm:ss');
		const maxDate = MomentTimezone(dateLimits.data.maxDate, 'YYYY-MM-DD HH:mm:ss');
		const retValue: MinMaxDatesDto = { "minDate": minDate, "maxDate": maxDate, "nbOfImages": totalNb.data };
		callback(retValue);
	}

}