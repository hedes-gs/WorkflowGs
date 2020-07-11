import axios, { AxiosInstance } from 'axios';
import { ServiceConfig } from './api.config'

export interface RatingsService {
    count(rating: number): Promise<number>;
    countAll(): Promise<string>;
}

export default class RatingsServiceImpl implements RatingsService {

    axiosInstance: AxiosInstance;
    baseUrlForRatings: string;


    constructor() {
        this.axiosInstance = axios.create(ServiceConfig);
        this.baseUrlForRatings = '/api/gs/ratings/count/';
    }

    async count(rating: number): Promise<number> {
        const urlToGetRating = this.baseUrlForRatings + rating;
        return this.axiosInstance.get(urlToGetRating)
            .then(resp => resp.data);
    }

    async countAll(): Promise<string> {
        const urlToGetRating = this.baseUrlForRatings + 'all';
        return this.axiosInstance.get(urlToGetRating)
            .then(resp => resp.data);
    }


}
