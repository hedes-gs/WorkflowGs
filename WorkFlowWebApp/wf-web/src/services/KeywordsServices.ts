import axios, { AxiosInstance } from 'axios';
import { ServiceConfig } from './api.config'
import { ExchangedImageDTO, toJsonExchangedImageDTO,toJsonImageDTO } from '../model/DataModel';

export interface KeywordsService {
    addKeyword(keyword: string, img: ExchangedImageDTO): Promise<string>;
    deleteKeyword(keyword: string, img: ExchangedImageDTO): Promise<string>;
    getKeywordsLike(keyword: string): Promise<string>;
    getAll(): Promise<string>;
}

export default class KeywordsServiceImpl implements KeywordsService {

    axiosInstance: AxiosInstance;
    baseUrlForKeywords: string;


    constructor() {
        this.axiosInstance = axios.create(ServiceConfig);
        this.baseUrlForKeywords = '/api/gs/keywords/';
    }

    async getKeywordsLike(keyword: string): Promise<string> {
        const urlToGetKeywordsLike = this.baseUrlForKeywords + 'getKeywordsLike/' + keyword;
        return this.axiosInstance.get(urlToGetKeywordsLike)
            .then(resp => resp.data);
    }

    async addKeyword(keyword: string, img: ExchangedImageDTO): Promise<string> {
        const urlToAddKeyword = this.baseUrlForKeywords + 'addToImage/' + keyword;

        return this.axiosInstance.post(urlToAddKeyword, toJsonImageDTO(img.image), {
            headers: {
                'Content-Type': 'application/json'
            }
        })
            .then(resp => resp.data);
    }

    async deleteKeyword(keyword: string, img: ExchangedImageDTO): Promise<string> {
        const urlToAddKeyword = this.baseUrlForKeywords + 'deleteInImage/' + keyword;

        return this.axiosInstance.post(urlToAddKeyword, toJsonImageDTO(img.image), {
            headers: {
                'Content-Type': 'application/json'
            }
        })
            .then(resp => resp.data);
    }

    async getAll(): Promise<string> {
        const urlToGetRating = this.baseUrlForKeywords + 'all';
        return this.axiosInstance.get(urlToGetRating)
            .then(resp => resp.data);
    }


}
